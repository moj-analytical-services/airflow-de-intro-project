import os
import boto3
import awswrangler as wr
import pandas as pd
from arrow_pd_parser import reader, writer
from datetime import datetime
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter
from dataengineeringutils3.s3 import (
    copy_s3_folder_contents_to_new_folder,
)

S3_BUCKET_NAME = "airflow-intro-test-murad"
S3_FOLDER_PATH = "loaded_data/"
CURATED_FOLDER_PATH = "curated_data/"
db_name = "curated_database"
data_s3_path = f"s3://{S3_BUCKET_NAME}/{CURATED_FOLDER_PATH}"
def load_files_from_s3():
    #Retrieve bucket name and folder path from environment variables
    bucket_name = os.getenv('S3_BUCKET_NAME')
    folder_path = os.getenv('S3_FOLDER_PATH')

    #Initialize boto3 S3 client
    s3 = boto3.client('s3', region_name='eu-west-2')
    data_frames = []
    # List objects within the specified folder
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_FOLDER_PATH)
    file_paths = [obj['Key'] for obj in response['Contents']]
    for file_path in file_paths:
        if file_path.endswith('/'):
            continue
        file_path = 's3://{}/{}'.format(S3_BUCKET_NAME,file_path)
        df = reader.csv.read(file_path)
        data_frames.append(df)
        full_df = pd.concat(data_frames, ignore_index=True)
        return full_df

    #s3://airflow-intro-test-murad/loaded_data/people-part1.parquet
def load_metadata() -> Metadata:
    metadata = Metadata.from_json("/Users/murad.ali/justice-pro/dmat/airflow-de-intro-project-murad-ali-MoJ/data/metadata/people.json")
    return metadata

def update_metadata() -> Metadata:
    metadata = load_metadata()
    new_columns = [

        {
            "name": "mojap_start_datetime",
            "type": "timestamp(s)",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "description": "Source extraction date"
        },
        {
            "name": "mojap_image_tag",
            "type": "string",
            "description": "Airflow Dag image tag"
        },
        {
            "name": "mojap_raw_filename",
            "type": "string",
            "description": "Name of the source file"
        },
         {
            "name": "mojap_task_timestamp",
            "type": "timestamp(s)",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "description": "Airflow Task was initiated time"
        },

    ]

    for column in new_columns:
        metadata.update_column(column)
    return metadata



def cast_columns_to_correct_types(df):
    metadata = update_metadata()
   
    for column in metadata.columns:
        column_name = column["name"]
        column_type = column["type"]

        if column_name not in df.columns:
            if column_type == "timestamp(ms)" or column_type == "timestamp(s)":
                df[column_name] = pd.NaT
            elif column_type == "string":
                df[column_name] = ""
            else:
                df[column_name] = pd.NA
        if column_type == "timestamp(ms)" or column_type == "timestamp(s)":
            df[column_name] = pd.to_datetime(
                df[column_name],
                format=column.get("datetime_format", "%Y-%m-%dT%H:%M:%S"),
            )
        else:
            df[column_name] = df[column_name].astype(column_type)
    return df

            

def add_mojap_columns_to_dataframe(df):

    # Add columns to dataframe
    #df["mojap_start_datetime"] = pd.to_datetime(df["source_extraction_date"])
    df["mojap_start_datetime"] = "source_extraction_date"
    #df["mojap_image_tag"] = globals().get("AIRFLOW_IMAGE_TAG", "")
    df["mojap_image_tag"] = "AIRFLOW_IMAGE_TAG"
    #df["mojap_raw_filename"] = globals().get("RAW_FILENAME", "")
    df["mojap_raw_filename"] = "RAW_FILENAME"
    #df["mojap_task_timestamp"] = pd.to_datetime(globals().get("AIRFLOW_TASK_TIMESTAMP", ""))
    df["mojap_task_timestamp"] = "AIRFLOW_TASK_TIMESTAMP"
    return df

    
#add_mojap_columns_to_dataframe(cast_columns_to_correct_types(load_files_from_s3()))
def write_curated_table_to_s3(df):
    glue_client = boto3.client('glue',  region_name='eu-west-2')
    db_name = "curated_database"
    metadata = update_metadata()
    gc = GlueConverter()
    tb_name = metadata.name
    data_s3_path = f"s3://{S3_BUCKET_NAME}/{CURATED_FOLDER_PATH}"
    parquet_path = f"{data_s3_path}{tb_name}.parquet"


    db_name = "curated_database"
    tb_name = "curated_table"
    # create database
    try:
        glue_client.create_database( 
            DatabaseInput = { 
                "Name": db_name
                }

        )
        print("Database created successfully")
    except glue_client.exceptions.AlreadyExistsException:
        print("Database already exists:", db_name)
    except Exception as e:
        print("An error occurred to creating database", e)

    writer.write(df=df, output_path=parquet_path, file_format="parquet")
    
    # create table
    try:
        glue_client.get_table(DatabaseName= db_name, Name = tb_name)
        print("table exist", tb_name, "Deleting table")
        glue_client.delete_table (DatabaseName= db_name, Name = tb_name)
    
    except Exception as e:
        print("Table doesn't exist")

    
    meta_dict = gc.generate_from_meta(metadata, database_name= db_name, table_location= data_s3_path)
    glue_client.create_table(**meta_dict)
    print( f"The {tb_name} has been created")

def athena_columns(meta: Metadata) -> dict[str, str]:
    """
    Return the column names and Athena types in a dictionary.
    """
    glue_meta = GlueConverter().generate_from_meta(
        meta, database_name="db_name", table_location="data_s3_path"
    )
    return {
        col["Name"]: col["Type"]
        for col in glue_meta["TableInput"]["StorageDescriptor"]["Columns"]
    }
metadata = load_metadata()
output = athena_columns(metadata)
print(output)
def move_completed_files_to_raw_hist():
    s3 = boto3.client('s3', region= 'eu-west-2')
    source_key = CURATED_FOLDER_PATH

    # Check if the destination folder exists, if not, create it
    destination_folder = 'raw_hist/'
    try:
        s3.head_object(Bucket=S3_BUCKET_NAME, Key=destination_folder)
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=destination_folder, Body='')
        else:
            raise

    source_path = f"s3://{S3_BUCKET_NAME}/{CURATED_FOLDER_PATH}"
    destination_path = f"s3://{S3_BUCKET_NAME}/{destination_folder}"
    copy_s3_folder_contents_to_new_folder(source_path,destination_path)

# move_completed_files_to_raw_hist()

write_curated_table_to_s3(add_mojap_columns_to_dataframe(cast_columns_to_correct_types(load_files_from_s3())))
