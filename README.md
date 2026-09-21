A PySpark batch-processing pipeline for a Resilient Global Telemetry Platform that demonstrates distributed data processing concepts using historical fleet telemetry data.

The project focuses on calculating vehicle-level and model-level engine temperature statistics while demonstrating important Apache Spark concepts including narrow and wide dependencies, shuffle operations, data skew mitigation using salting, partitioning strategies, and checkpointing for fault-tolerant iterative computations.

📌 Project Overview

Fleet telemetry systems generate large volumes of data from vehicles, including engine temperature, speed, location, battery level, and timestamps.

This project demonstrates how PySpark can be used to process this telemetry data efficiently while addressing common distributed-computing challenges such as:

Data skew

Expensive shuffle operations

Uneven workload distribution

Deep transformation lineage

Fault recovery

Partitioning strategy

The pipeline reads historical telemetry data from HDFS, performs transformations and aggregations, mitigates skewed vehicle IDs using salting, and uses checkpointing to control lineage growth during iterative processing.

🏗️ Pipeline Architecture

Historical Telemetry Data,          
      Data Ingestion,
      Data Cleaning,
      Engine Temperature,
      Transformation
          
 
  Model-Level Aggregation 
      groupBy()           
 
              ↓
      Vehicle-Level
      Skew Mitigation
              ↓
          Salting
              ↓
      Partial Aggregation
              ↓
       Final Aggregation
              ↓
     Iterative Computation
              ↓
       Checkpointing
              ↓
    Degradation Scores
              ↓
        HDFS Output

📊 Input Data

The pipeline expects historical telemetry data in Parquet format stored in HDFS.

The telemetry schema contains:

Column

Type

Description

vehicle_id

String

Unique vehicle identifier

vehicle_model

String

Vehicle model

event_ts

Timestamp

Telemetry event timestamp

engine_temp_c

Double

Engine temperature in Celsius

speed_kph

Double

Vehicle speed

lat

Double

Latitude

lon

Double

Longitude

battery_pct

Double

Battery percentage

The schema is explicitly defined using PySpark StructType and StructField definitions.

🔄 Data Processing Pipeline

1. Data Ingestion

Historical telemetry is loaded from HDFS using a predefined schema:

hdfs:///data/telemetry/historical/

The pipeline uses Spark's Parquet reader for distributed data ingestion.

2. Data Cleaning & Transformation

Records with missing engine-temperature values are filtered out.

The pipeline also converts engine temperature from Celsius to Fahrenheit:

engine_temp_f = engine_temp_c × 9/5 + 32

These operations represent narrow transformations, where each output partition depends on a single input partition and no network shuffle is required.

📈 Model-Level Aggregation

The pipeline calculates the average engine temperature for each vehicle model.

It also calculates the number of telemetry readings:

groupBy(vehicle_model)
        ↓
Average Engine Temperature
        +
Reading Count

The groupBy() operation creates a wide dependency because records belonging to the same vehicle model must be moved across partitions through a shuffle.

⚖️ Data Skew Mitigation Using Salting

One of the key concepts demonstrated in this project is data skew mitigation.

Some vehicles may generate significantly more telemetry records than other vehicles. If a normal:

groupBy("vehicle_id")

operation is used, all records for a highly active vehicle can be sent to the same reduce partition.

This can create a straggler task, where one task performs significantly more work than the others.

Salting Strategy

The pipeline creates a random salt value for each record:

vehicle_id + salt

with:

20 salt buckets

This effectively splits a heavily skewed vehicle into multiple sub-keys.

Two-Stage Aggregation

The aggregation is performed in two stages:

Raw Telemetry
      ↓
Salted Vehicle ID
      ↓
Partial Aggregation
      ↓
Recombine Salted Results
      ↓
Final Average per Vehicle

The first aggregation calculates partial sums and counts for each salted key. The second aggregation combines these partial results to calculate the final average per vehicle.

This reduces the amount of work concentrated on a single partition for highly skewed vehicle IDs.

🔀 Partitioning Strategies

The project also demonstrates different partitioning approaches.

Hash Partitioning

The salted dataset is repartitioned using the salted key:

repartition(SALT_BUCKETS * 10, "salted_key")

This distributes salted keys across multiple partitions.

Range Partitioning

The pipeline also demonstrates range partitioning by telemetry timestamp:

repartitionByRange(50, "event_ts")

Range partitioning can be useful for workloads involving ordered time-series processing.

💾 Checkpointing & Lineage Management

The project demonstrates Spark checkpointing during a simulated iterative predictive-maintenance computation.

Each iteration derives a new DataFrame from the previous iteration. Without checkpointing, the transformation lineage can become increasingly deep.

The pipeline performs 300 iterations and checkpoints the DataFrame periodically.

Why Checkpointing?

A long lineage can make recomputation expensive because Spark may need to replay a large number of transformations when recovering lost data.

Checkpointing truncates the lineage by writing the current state to reliable storage and allowing future computations to reference the checkpointed data instead of the entire transformation history.

The pipeline checkpoints every 20 iterations after first caching and materializing the DataFrame.

🧮 Simulated Degradation Score

The iterative computation creates a simulated degradation_score based on average engine temperature and the current iteration.

Conceptually:

degradation_score
        =
average engine temperature × 0.001
        +
iteration × 0.0001

The final scores are written back to HDFS in Parquet format.

🛡️ Fault Tolerance

The pipeline configures a checkpoint directory in HDFS:

hdfs:///checkpoints/fleet_telemetry

Checkpoint data is intended to be stored on reliable, replicated storage so that it can survive individual node failures in a real cluster environment.

⚙️ Spark Configuration

The Spark session is configured with:

Application Name:
FleetTelemetryBatchProcessing

Shuffle Partitions:
200

The pipeline uses Apache Spark's distributed execution engine for processing the telemetry workload.

🛠️ Technologies Used

Python

Apache Spark

PySpark

Spark SQL

HDFS

Parquet

Distributed Data Processing

🧠 Key Concepts Demonstrated

This project provides hands-on implementation of:

PySpark DataFrame operations

Distributed batch processing

Narrow dependencies

Wide dependencies

Spark shuffle

Hash partitioning

Range partitioning

Data skew

Salting

Two-stage aggregation

Spark caching

Spark checkpointing

RDD/DataFrame lineage

Fault tolerance

HDFS-based storage

Parquet processing

Iterative Spark computations

📂 Project Structure

resilient-telemetry-platform/

telemetry_pipeline.py

README.md

Assignment_Resilient_Telemetry_Platform.md

The accompanying document provides additional explanation of narrow versus wide dependencies, RDD lineage, fault tolerance, and checkpointing versus caching.

