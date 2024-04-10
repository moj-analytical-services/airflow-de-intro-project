import os
import shutil
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse

import boto3
import awswrangler as wr
import pandas as pd
import pyarrow.parquet as pq
import pydbtools as pydb
from botocore.exceptions import ClientError
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter
from arrow_pd_parser import reader, writer
from typing import Dict, Union, Optional

from config import settings


def load_data_from_s3(partitions=None):
    s3_path = (
            "s3://dami-test-bucket786567/de-intro/land/"
        )
    s3 = boto3.resource('s3')
    bucket_name, prefix = s3_path.replace('s3://', '').split('/', 1)
    bucket = s3.Bucket(bucket_name)

    dfs = []
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith('.parquet'):
            if partitions and not any(partition in obj.key for partition in partitions):
                continue

            parquet_file = 's3://{}/{}'.format(bucket_name, obj.key)
            df_frag = pq.ParquetDataset(parquet_file).read().to_pandas() #, use_legacy_dataset=False
            dfs.append(df_frag)

    full_df = pd.concat(dfs, ignore_index=True)
    return full_df



def load_metadata() -> Metadata:
    metadata_path = 'data/metadata/people.json'
    metadata = os.path.join(os.getcwd(), metadata_path)
    metadata = Metadata.from_json(metadata)
    return metadata

def update_metadata() -> Metadata:

    metadata = load_metadata()
    new_columns = [
        {
            "name": "mojap_start_datetime",
            "type": "timestamp(s)",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "description": "extraction start date",
        },
        {
            "name": "mojap_image_tag",
            "type": "string",
            "description": "image version",
        },
        {
            "name": "mojap_raw_filename",
            "type": "string",
            "description": "",
        },
        {
            "name": "mojap_task_timestamp",
            "type": "timestamp(s)",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "description": "",
        },
    ]
    for new_column in new_columns:
        metadata.update_column(new_column)
    return metadata

def cast_columns_to_correct_types(df):
    metadata = update_metadata()

    for column in metadata.columns:
        column_name = column["name"]
        column_type = column["type"]

        if column_name not in df.columns:
            if column_type == "timestamp(s)":
                df[column_name] = pd.NaT
            elif column_type == "string":
                df[column_name] = ""
            else:
                df[column_name] = pd.NA

        if column_type == "timestamp(s)":
            df[column_name] = pd.to_datetime(
                df[column_name],
                format=column.get(
                    "datetime_format", "%Y-%m-%dT%H:%M:%S"
                ),
            )
        else:
            df[column_name] = df[column_name].astype(
                column_type
            )
    return df


def add_mojap_columns_to_dataframe(df):

    df["mojap_start_datetime"] = pd.to_datetime(df["Source extraction date"])
    df["mojap_image_tag"] = settings.MOJAP_IMAGE_VERSION
    df["mojap_raw_filename"] = "people-100000.csv"
    df["mojap_task_timestamp"] = pd.to_datetime(
        settings.MOJAP_EXTRACTION_TS, unit='s'
    )
    return df


def write_curated_table_to_s3(df, overwrite_table: Optional[bool] = True):
    
    # Parameters
    db_dict: Dict[str, Union[str, None]] = {'name': "dami_intro_project",
           'description': 'database with data from people parquet',
           'table_name': 'people',
           'table_location': "s3://dami-test-bucket786567/de-intro/"
           }
    db_meta: Dict[str, Union[str, None]] = {
                "DatabaseInput": {
                    "Description": db_dict['description'],
                    "Name": db_dict['name']
                }
            }
    
    # Load metadata
    metadata = update_metadata()

    gc = GlueConverter()
    glue_client = boto3.client("glue", region_name="eu-west-1")

    # Create Database
    try:
        glue_client.get_database(Name=db_dict['name'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            glue_client.create_database(**db_meta)
        else:
            print(f"Unexpected error: {e}")

    # Create Table
    try:
        table_exists = True
        glue_client.get_table(DatabaseName=db_dict['name'], Name=db_dict['table_name'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            table_exists = False
        else:
            print(f"Unexpected error: {e}")

    if table_exists:
        if overwrite_table:
            print(f"Deleting existing table - '{db_dict['table_name']}' in database - '{db_dict['name']}'")
            
            wr.catalog.delete_table_if_exists(database=db_dict['name'], table=db_dict['table_name'])
            spec = gc.generate_from_meta(metadata, database_name=db_dict['name'], 
                                         table_location=db_dict['table_location'])
            glue_client.create_table(**spec)

            print(f"Table {db_dict['table_name']} created in database {db_dict['name']}")
        else:
            print(f"Table {db_dict['table_name']} already exists in database {db_dict['name']}")
    else:
        spec = gc.generate_from_meta(metadata, database_name=db_dict['name'], table_location=db_dict['table_location'])
        glue_client.create_table(**spec)
        print(f"Table {db_dict['table_name']} created in database {db_dict['name']}")


    file_path = s3_path_join(
            db_dict['table_location'], f"{db_dict['table_name']}.parquet" #{time.time_ns()}
        )
    writer.write(
        df=df,
        output_path=file_path,
        metadata=athena_columns(metadata),
        file_format="parquet",
    )

    # Register the table with the Glue Catalog
    wr.catalog.create_parquet_table(
        database=db_dict['name'],#"dami_intro_project",
        table=db_dict['table_name'],
        path= file_path, #db_dict['table_location'],
        columns_types=athena_columns(metadata),
        description="Curated people data",
    )


def move_completed_files_to_raw_hist():

    land_folder = 'data/folder/land'
    raw_hist_folder = 'data/folder/raw_hist'

    # Create Raw_Hist folder if it doesn't exist
    if not os.path.exists(raw_hist_folder):
        os.makedirs(raw_hist_folder)

    # Loop through files in Land folder
    for filename in os.listdir(land_folder):
        src_path = os.path.join(land_folder, filename)
        dst_path = os.path.join(raw_hist_folder, filename)

        # Check if the file has completed processing (add your logic here)
        if file_processing_completed(filename):
            # Move the file from Land to Raw_Hist
            shutil.move(src_path, dst_path)
            print(f"Moved {filename} to {raw_hist_folder}")
        else:
            print(f"{filename} is still being processed")


def file_processing_completed(filename):

    # Example: Check if a flag file exists
    flag_file = f"{filename}.processed"
    return os.path.exists(flag_file)


def apply_scd2():

    # Load the data files
    people_part1 = pd.read_parquet('path/to/people-part1.parquet')
    people_part2 = pd.read_parquet('path/to/people-part2.parquet')
    people_part3 = pd.read_parquet('path/to/people-part3.parquet')

    # Concatenate the data frames
    people_df = pd.concat([people_part1, people_part2, people_part3])

    # Sort the data frame by user ID and mojap_start_datetime
    people_df = people_df.sort_values(['user_id', 'mojap_start_datetime'])

    # Apply SCD2 logic
    scd2_df = apply_scd2_logic(people_df)

    # Save the result to a new file
    scd2_df.to_parquet('path/to/people_scd2.parquet', index=False)


def apply_scd2_logic(df):

    scd2_df = df.copy()
    scd2_df['row_id'] = range(1, len(scd2_df) + 1)

    # Add a flag column to identify the current row
    scd2_df['is_current'] = False
    scd2_df.loc[scd2_df.groupby('user_id')['mojap_start_datetime'].idxmax(), 'is_current'] = True

    return scd2_df

# ---------------------------

def s3_path_join(base: str, *url_parts: str) -> str:
    """
    Joins a base S3 path and URL parts.
    Args:
        base (str): Base S3 URL
        url (str): one or more URL parts
    Example:
    s3_path_join("s3://bucket", "folder", "subfolder") # s3://bucket/folder/subfolder
    """

    def ensure_folder(path):
        return path + "/" if not path.endswith("/") else path

    for url in [ensure_folder(url_part) for url_part in url_parts[:-1]] + list(
        url_parts[-1:]
    ):
        base_path = urlparse(ensure_folder(base))
        base = urlunparse(base_path._replace(path=urljoin(base_path.path, url)))
    return base


def athena_columns(meta: Metadata) -> Dict[str, str]:
    """
    Return the column names and Athena types in a dictionary.
    """
    glue_meta = GlueConverter().generate_from_meta(
        meta, database_name="dummy", table_location="dummy"
    )
    return {
        col["Name"]: col["Type"]
        for col in glue_meta["TableInput"]["StorageDescriptor"]["Columns"]
    }

