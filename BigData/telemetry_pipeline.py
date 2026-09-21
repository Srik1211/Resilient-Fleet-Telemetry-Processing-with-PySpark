"""
PySpark batch pipeline for Part 3 of the Resilient Global Telemetry Platform
assignment.

Computes average engine temperature per vehicle model from a historical
telemetry batch, mitigates data skew from a small number of "hot" delivery
trucks via salting, and demonstrates checkpointing to bound lineage depth
during a simulated iterative computation.

See Assignment_Resilient_Telemetry_Platform.md for the accompanying
explanation of narrow vs. wide dependencies, fault tolerance via RDD
lineage, and the checkpointing-vs-caching distinction.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

spark = (
    SparkSession.builder
    .appName("FleetTelemetryBatchProcessing")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

# Reliable, replicated storage is required here -- checkpoint data must
# survive individual node failures, so a local filesystem path is not
# sufficient in a real cluster deployment.
spark.sparkContext.setCheckpointDir("hdfs:///checkpoints/fleet_telemetry")


# ---------------------------------------------------------------------------
# 1. Ingest historical telemetry + average engine temp per vehicle model
# ---------------------------------------------------------------------------
schema = StructType([
    StructField("vehicle_id", StringType(), False),
    StructField("vehicle_model", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("speed_kph", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("battery_pct", DoubleType(), True),
])

telemetry_df = (
    spark.read
    .schema(schema)
    .parquet("hdfs:///data/telemetry/historical/")
)

# NARROW dependencies: each output partition is computed from exactly one
# input partition. No data crosses the network, so Spark pipelines these
# into a single stage.
clean_df = (
    telemetry_df
    .filter(F.col("engine_temp_c").isNotNull())                       # narrow
    .withColumn("engine_temp_f", F.col("engine_temp_c") * 9 / 5 + 32)  # narrow
)

# WIDE dependency: groupBy requires every row for a given vehicle_model to
# land on the same reduce-side partition, regardless of which input
# partition it started in. This forces a shuffle and starts a new stage.
avg_temp_by_model = (
    clean_df
    .groupBy("vehicle_model")
    .agg(
        F.avg("engine_temp_c").alias("avg_engine_temp_c"),
        F.count("*").alias("reading_count"),
    )
)

avg_temp_by_model.show()


# ---------------------------------------------------------------------------
# 2. Skew mitigation via salting
#
#    A handful of vehicle_ids emit ~1000x the log volume of a typical
#    truck. A plain groupBy("vehicle_id") hashes every row for that
#    vehicle_id to the SAME reduce partition, so one task ends up doing
#    1000x the work of its peers (a "straggler" that stalls the whole
#    stage). Salting artificially fans a hot key out across many
#    sub-partitions so the shuffle load is spread evenly.
# ---------------------------------------------------------------------------
SALT_BUCKETS = 20  # tune based on skew severity and available cores

salted_df = (
    clean_df
    .withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int"))
    .withColumn("salted_key", F.concat_ws("_", F.col("vehicle_id"), F.col("salt")))
)

# Explicit hash partitioning on the salted key: rows for a hot vehicle_id
# are now spread across SALT_BUCKETS distinct keys, so the default hash
# partitioner sends them to SALT_BUCKETS different reduce tasks instead
# of one.
salted_df = salted_df.repartition(SALT_BUCKETS * 10, "salted_key")

# Stage 1 -- partial aggregation on the salted key. Each of the
# SALT_BUCKETS sub-keys for a hot vehicle_id now does roughly 1/SALT_BUCKETS
# of the original work.
partial_agg = (
    salted_df
    .groupBy("vehicle_id", "salt")
    .agg(
        F.sum("engine_temp_c").alias("sum_temp"),
        F.count("*").alias("cnt"),
    )
)

# Stage 2 -- recombine the salted partials into one row per real
# vehicle_id. This second shuffle is cheap: partial_agg has at most
# SALT_BUCKETS rows per vehicle_id instead of millions of raw readings.
final_avg_per_vehicle = (
    partial_agg
    .groupBy("vehicle_id")
    .agg((F.sum("sum_temp") / F.sum("cnt")).alias("avg_engine_temp_c"))
)

# If a downstream step needs ordered scans instead of point lookups by key
# (e.g. per-vehicle time-series window functions for predictive
# maintenance), range partitioning on the timestamp keeps sorted runs
# contiguous within a partition, which hash partitioning does not:
range_partitioned = clean_df.repartitionByRange(50, "event_ts")


# ---------------------------------------------------------------------------
# 4. Checkpointing -- truncating a runaway lineage graph
#
#    Simulates an iterative predictive-maintenance score that updates its
#    state once per telemetry batch (e.g. a running battery-degradation
#    estimate). Each iteration's DataFrame is derived from the previous
#    one, so after hundreds of iterations the lineage (logical plan)
#    grows hundreds of transformations deep. Recomputing a lost partition
#    means walking that entire chain, which risks StackOverflowError
#    during plan traversal and makes recovery slow (replay everything
#    since the source data, not just the last few steps).
# ---------------------------------------------------------------------------
running_score_df = avg_temp_by_model  # iteration 0 baseline

for i in range(300):
    running_score_df = running_score_df.withColumn(
        "degradation_score",
        F.col("avg_engine_temp_c") * 0.001 + F.lit(i) * 0.0001,
    )

    if i % 20 == 0:
        # Persist first so checkpoint() doesn't recompute the whole chain
        # from source just to materialize the write.
        running_score_df = running_score_df.cache()
        running_score_df.count()  # force materialization of the cache

        # checkpoint() writes the current DataFrame to the reliable,
        # replicated checkpoint directory and replaces its lineage with a
        # single "read this checkpoint file" node. Every iteration after
        # this point depends on at most 20 prior transformations, not the
        # full 300-iteration history.
        running_score_df.checkpoint()

final_scores = running_score_df
final_scores.write.mode("overwrite").parquet("hdfs:///data/telemetry/degradation_scores/")

spark.stop()
