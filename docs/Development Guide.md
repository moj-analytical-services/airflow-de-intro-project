### load data from s3 ###

In this stage we need to load our dataset from an s3 bucket
and return it to the rest of the pipeline in the form of
a Pandas DataFrame.
Original data source: https://github.com/datablist/sample-csv-files
Source file: "people-100000.csv"
To do this we will utilise the read() method of an arrow_pd_parser
reader object, this method will require the path to the data
object on s3
Arrow PD Parser:
https://github.com/moj-analytical-services/mojap-arrow-pd-parser



### cast columns to correct types ###

In this stage we need to compare the data types of each column in our
pandas dataframe and ensure they match with the expected
types from the provided Mojap Metadata, casting types as required.
Mojap Metadata:
https://github.com/moj-analytical-services/mojap-metadata


### add mojap columns to dataframe ###

In this stage we need to add a set of columns to the dataframe and
metadata which are derived from environment variables, this will allow
traceability for when entries were added, which version of the pipeline
was used.
To add entries to the Metadata we utilise the update_column() method of
the metadata object.
Once this is done, columns should be added to the dataframe ensuring the
correct data type is used.


#### The columns we will add are: ####
"mojap_start_datetime" - This is derived from the "Source extraction date"
column in the provided data. This will be used for scd2 in the future.
"mojap_image_tag" - This is the release version of the script being used
to produce the curated table. This value should be passed as an environment
variable from the Airflow Dag.
"mojap_raw_filename" - This is the name of the source file the data originated
in.
"mojap_task_timestamp" - This is the time the Airflow Task was initiated. This
should be passed as an environment variable from the Airflow Dag.


Once all transformations on the dataframe are completed we need to write
the data to an appropriate s3 bucket written in .parquet format.
An Arrow PD Parser writer object can be used to acheive this.
The table should then be registered with the Glue Catalogue.
AWS Wrangler can be used to acheive this:
https://github.com/aws/aws-sdk-pandas
specifically the create_parquet_table method of the Catalogue module.
Before this, we will need to convert the metadata to an appropriate format
using a GlueConverter object from Mojap Metadata


### move completed files to raw hist ###

When all processing on a file has successfully completed and that data has
been written, that file should be moved from the Land folder to the
Raw Hist folder. This allows us to maintain a history of all data sources
over time, enabling rebuild of our databases in the case of errors being
identified.


### apply scd2 ###

The third data file "people-part3.parquet" contains updates to some entries
in the first two data files. To allow for 'rewinding' of the database to
an earlier state, important for reproduceability, we need to apply scd2
(slowly changing dimension Type 2 - 
https://en.wikipedia.org/wiki/Slowly_changing_dimension) based on the
mojap_start_datetime column.
The updated rows are for user ID 'e09c4f4cbfEFaFd',
'eda7EcaF87b2D80' and '9C4Df1246ddf543'
Further instructions TBC.
"""
