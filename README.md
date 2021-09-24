# Overview

- Parse `.parquet` file to `.avro` format.

## Setup

- Get `Java 8` installed
- Build jar with `./gradlew clean jar`
- Place `parquet-to-avro-java-1.0-SNAPSHOT.jar` in a folder along with 2 folders: `input` and `output` (you can take samples from `resources/data/input` folder)

## Use cases

- Parse `employee_sample_data1.parquet` to `avro` format (infer `avro schema` from `parquet`)
    - result: avro get the same schema as in parquet
- Parse `sample_data_3columns.parquet` file (3 columns) to `avro` format by reading avro schema (schema `avro_schema_3columns.avsc`)
    - result: avro get the same schema as provided .avsc file
- Parse `sample_data_4columns.parquet` file (4 columns) to `avro` format by reading avro schema (schema `avro_schema_3columns.avsc`)
    - result: 4th column ignored (`available` property) -> avro output with 3 columns (same as schema) 
- Parse `sample_data_3columns.parquet` file (3 columns) to `avro` format by reading avro schema (schema `avro_schema_4columns.avsc`)
    - result: cannot set `available` property as default in avro [TODO]

## How to run

- `java -jar parquet-to-avro-java-1.0-SNAPSHOT.jar "path/to/somefile.parquet" "path/to/somefile.avro" "path/to/someschemafile.avsc"`
    - 3rd parameter (avro schema) is optional

## TODO

- reading `.parquet` with 4 lines, but getting `.avro` with 3 lines (`ParquetReader.read()` skips index 0)
- `.parquet` with 3 columns and `avro schema` with 4 columns -> error -> Cannot get index 3, and it doesn't set default value

