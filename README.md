# Data Migration Challenge

## Overview
This application consists of a simple pipeline to perform a data migration. It takes CSV files with specific schemas as input and inserts the content into hive tables in AVRO format. 
It uses PySpark and Airflow throughout the process.


## Platforms and tools
The data will be stored using Hive-Hadoop. The 3 tables used for the migration can be created with the scripts contained in the folder `create_table_scripts`.

The pipeline runs on Airflow. A Spark Operator takes care of reading the data from CSV files. It performs a data types check, and after confirming the compatibility, the data is inserted into the corresponding tables.

## Scheduling and file handling 
The pipeline runs every day at 12 noon. It checks for new files in the folder `input/`. After reading each file, if the file data types were compatible and the data was inserted, the file is moved to the folder `processed/`. On the other hand, if the file data types were incorrect, the file is moved to the folder `skipped`.

## Stack / Technologies
- Pyspark
- SQL
- Hadoop
- Airflow
- CSV
- AVRO