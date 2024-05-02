import os
import time
import logging
from datetime import datetime

import boto3
from botocore.exceptions import BotoCoreError, ClientError

import pandas as pd
import pyarrow.parquet as pq
import awswrangler as wr

import pydbtools as pydb

from urllib.parse import urlparse
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter
from arrow_pd_parser import reader, writer

from typing import Dict, Union, Optional, Tuple

from .config import settings
from .utils import s3_path_join
from dataengineeringutils3.s3 import get_filepaths_from_s3_folder

# Set up logging
logging.basicConfig(filename='data_pipeline.log', level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

def extract_data_to_s3():
    s3_path = settings.LANDING_FOLDER
    base = os.path.join(os.getcwd(), 'data/example-data')
    for root, _, files in os.walk(base):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                s3_file_path = os.path.join(s3_path, file)
                wr.s3.upload(file_path, s3_file_path)
                logging.info(f"Uploading {file} to {s3_path}")
    logging.info("Extraction complete")
    print("Extraction complete")



def load_data_from_s3(partitions: Optional[Dict[str, str]] = None):

    s3_path = settings.LANDING_FOLDER
    s3 = boto3.resource("s3")
    bucket_name, prefix = (s3_path).replace("s3://", "").split("/", 1)
    bucket = s3.Bucket(bucket_name)

    dfs = []
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith(".parquet"):
            if partitions and not any(partition in obj.key for partition in partitions):
                continue
            parquet_file = "s3://{}/{}".format(bucket_name, obj.key)
            df_frag = (
                pq.ParquetDataset(parquet_file).read().to_pandas()
            )  # , use_legacy_dataset=False
            dfs.append(df_frag)
    full_df = pd.concat(dfs, ignore_index=True)
    return full_df



def load_metadata() -> Metadata:
    metadata = s3_path_join(
        settings.METADATA_FOLDER, f"{settings.TABLES}.json"
        )
    metadata = Metadata.from_json(metadata)
    # Affirm Table name
    metadata.name = settings.TABLES
    return metadata

def update_metadata() -> Metadata:

    metadata = load_metadata()
    new_columns = [
        {
            "name": "mojap_start_datetime",
            "type": "timestamp(ms)",
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
            "type": "timestamp(ms)",
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
            if column_type == "timestamp(ms)":
                df[column_name] = pd.NaT
            elif column_type == "string":
                df[column_name] = ""
            else:
                df[column_name] = pd.NA
        if column_type == "timestamp(ms)":
            df[column_name] = pd.to_datetime(
                df[column_name],
                format=column.get("datetime_format", "%Y-%m-%dT%H:%M:%S"),
            )
        else:
            df[column_name] = df[column_name].astype(column_type)
    return df


def add_mojap_columns_to_dataframe(df):
    df["mojap_start_datetime"] = pd.to_datetime(df["Source extraction date"])
    df["mojap_image_tag"] = settings.MOJAP_IMAGE_VERSION
    df["mojap_raw_filename"] = "people-100000.csv"
    df["mojap_task_timestamp"] = pd.to_datetime(
        settings.MOJAP_EXTRACTION_TS, unit='s'
    )
    return df


def write_curated_table_to_s3(df: pd.DataFrame):
    # Parameters
    db_dict: Dict[str, Union[str, None]] = {
        "name": "dami_intro_project",  # database name
        "description": "database with data from people parquet",
        "table_name": settings.TABLES,
        "table_location": settings.CURATED_FOLDER
    }

    db_meta: Dict[str, Union[str, None]] = {
        "DatabaseInput": {
            "Name": db_dict["name"],
            "Description": db_dict["description"],
        }
    }

    # Load metadata
    metadata = update_metadata()
    gc = GlueConverter()
    glue_client = boto3.client("glue")

    # Create Database
    try:
        glue_client.get_database(Name=db_dict["name"])
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            glue_client.create_database(**db_meta)
        else:
            logging.error("Unexpected error while accessing database '%s': %s", db_dict["name"], e)

    # Write parquet to curated_folder
    file_path = s3_path_join(db_dict["table_location"], f"{db_dict['table_name']}.parquet")

    # Write the parquet file
    writer.write(df=df, output_path=file_path, metadata=metadata, file_format="parquet")

    # Create or overwrite the table
    try:
        glue_client.delete_table(DatabaseName=db_dict["name"], Name=db_dict["table_name"])
        logging.info("Deleted existing table '%s' in database '%s'", db_dict["table_name"], db_dict["name"])
        time.sleep(5)
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            logging.error("Failed to delete table '%s' in database '%s': %s", db_dict["table_name"], db_dict["name"], e)

    spec = gc.generate_from_meta(metadata, database_name=db_dict["name"], table_location=db_dict["table_location"])
    glue_client.create_table(**spec)
    logging.info("Table '%s' created (or overwritten) in database '%s'", db_dict["table_name"], db_dict["name"])

    logging.info("Writing Data to Table '%s' in database '%s'", db_dict["table_name"], db_dict["name"])
    print(("Curation complete"))



def move_completed_files_to_raw_hist():
    land_folder = settings.LANDING_FOLDER
    raw_hist_folder = settings.RAW_HIST_FOLDER

    land_files = get_filepaths_from_s3_folder(s3_folder_path=land_folder)
    if not land_files:
        logging.info("No files to move into the landing folder - %s", land_folder)
        return
    # file_to_move = [urlparse(files).path.split('/')[-1].split('.') for files in land_files]
    
    target_path = s3_path_join(raw_hist_folder, f"dag_run_ts_{settings.MOJAP_EXTRACTION_TS}")
    logging.info("Target path for moved files: %s", target_path)

    try:
        wr.s3.copy_objects(
            paths=land_files,
            source_path=land_folder,
            target_path=target_path
        )
        logging.info("Files successfully moved from the landing folder to the raw history folder.")
    except (BotoCoreError, ClientError) as error:
        logging.error("Failed to move files: %s", error)
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)

    try:
        wr.s3.delete_objects(path=land_files)
        logging.info("Successfully deleted files in %s", land_folder)
    except (BotoCoreError, ClientError) as error:
        logging.error("Failed to delete files: %s", error)
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)


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

