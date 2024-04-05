import os
import shutil
from datetime import datetime

import pandas as pd
import awswrangler as wr
from mojap_metadata import Metadata
from arrow_pd_parser import reader
from typing import  Dict, Union, Optional

from config import settings


def load_data_from_s3(LOCAL=True, s3_url: Optional[str] = None) -> pd.DataFrame:
    if LOCAL:
        current_dir = os.getcwd()
        relative_path = os.path.join("data", "people-1000.csv")
        s3_path = os.path.join(current_dir, relative_path)
        df = reader.read(s3_path, file_format="csv")
    else:
        if s3_url is None:
            raise ValueError("s3_url must be provided when LOCAL=False")
        s3_path = s3_url
        df = reader.read(s3_path)

    return df

def load_metadata() -> Metadata:
    metadata = 'data\metadata\people.json'
    metadata = Metadata.from_json(metadata)
    return metadata

def update_metadata() -> Metadata:
     
    metadata = load_metadata()
    new_columns = [
        {"name": "mojap_start_datetime", "type": "timestamp(s)", 
         "datetime_format": "%Y-%m-%dT%H:%M:%S", "description": "extraction start date"},
        {"name": "mojap_image_tag", "type": "string", "description": "image version"},
        {"name": "mojap_raw_filename", "type": "string", "description": ""},
        {"name": "mojap_task_timestamp", "type": "timestamp(s)", 
         "datetime_format": "%Y-%m-%dT%H:%M:%S", "description": ""}
    ]   
    for new_column in new_columns:
        metadata.update_column(new_column)   
    return metadata

def cast_columns_to_correct_types(df):
    metadata = update_metadata()
    
    for column in metadata.columns:
        column_name = column['name']
        column_type = column['type']
        
        # handle potential instances of missing/new columns
        if column_name not in df.columns:
            if column_type == 'timestamp(s)':
                df[column_name] = pd.NaT 
            elif column_type == 'string':
                df[column_name] = ''
            else:
                df[column_name] = pd.NA 
                
        # cast the column to the correct type
        if column_type == 'timestamp(s)':
            df[column_name] = pd.to_datetime(df[column_name], format=column.get('datetime_format', "%Y-%m-%dT%H:%M:%S"))
        else:
            df[column_name] = df[column_name].astype(column_type)
    return df


def add_mojap_columns_to_dataframe(df):

    df["mojap_start_datetime"] = pd.to_datetime(df["Source extraction date"])
    df["mojap_image_tag"] = settings.AIRFLOW_IMAGE_TAG
    df["mojap_raw_filename"] = "people-100000.csv"
    df["mojap_task_timestamp"] = pd.to_datetime(
        settings.AIRFLOW_TASK_TIMESTAMP
    )
    return df


def write_curated_table_to_s3(df):
    # Convert metadata to Glue schema
    glue_converter = Metadata.GlueConverter()
    glue_schema = glue_converter.to_glue_schema(metadata)



    # Write DataFrame to S3 in Parquet format
    wr.s3.to_parquet(
        df=df,
        path="s3://bucket/curated/people/",
        dataset=True,
        database="mojap_db",
        table="people_curated",
        partition_cols=["mojap_start_datetime"],
        mode="overwrite",
        schema_evolution=True,
        dataset_type="parquet",
        description="Curated people data",
        parameters={"curated": "true"}
    )

    # Register the table with the Glue Catalog
    wr.catalog.create_parquet_table(
        database="mojap_db",
        table="people_curated",
        path="s3://bucket/curated/people/",
        schema=glue_schema,
        partition_cols=["mojap_start_datetime"],
        description="Curated people data",
        parameters={"curated": "true"}
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

