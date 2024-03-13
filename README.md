# airflow-de-intro-project
This repository presents a simple introductory project demonstrating the basic elements of a pipeline designed to ingest data in the form of parquet files, perform certain transformation on that data using Pandas before writing the output as an iceberg table within the s3 sandbox.

During the project you will use some basic functions from some internal MoJ packages.

All data in this repository was adapted from a synthetic open dataset originally located at: https://github.com/datablist/sample-csv-files

## How to use (WIP)

Initially clone this repo.

In scripts/functions.py we have defined some basic abstract functions which cover the main stages required for this pipeline with instructions in the doc string suggesting tools which should be used to perform these actions. 

If your implementation of these functions becomes too complex it may be necessary to define your own functions as intermediate steps to make it easier to read and test.

We will be updating this project based upon user feedback.
