import os
import boto3
import pandas as pd
from pathlib import Path
from arrow_pd_parser import reader, writer
from mojap_metadata import Metadata
import awswrangler as wr

def load_files_from_s3():
    # Retrieve bucket name and folder path from environment variables
    bucket_name = os.getenv('S3_BUCKET_NAME')
    folder_path = os.getenv('S3_FOLDER_PATH')

    # Initialize boto3 S3 client
    s3 = boto3.client('s3')

    # List objects within the specified folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

    # Extract file paths
    file_paths = [obj['Key'] for obj in response['Contents']]

    # Load each file into a DataFrame
    data_frames = []
    for file_path in file_paths:
        # Skip folder itself
        if file_path.endswith('/'):
            continue
        # Create each of the parquet file path
        parquet_path = 's3://{}/{}'.format(bucket_name,file_path)
        df = reader.parquet.read(parquet_path)
        data_frames.append(df)

    return pd.concat(data_frames, ignore_index=True)
    

def cast_columns_to_correct_types(df):

    # Load metadata
    metadata_path = "data/metadata/intro-project-metadata.json"
    meta = Metadata.from_json(metadata_path)
    
     # Get column names and expected types from metadata
    for col in df.columns:
    # Check if the column exists in metadata
        if col in meta.column_names:
            # Get the metadata for the column
            col_metadata = meta[col]
            if df[col].dtype != col_metadata["type"]:
                if col_metadata["type"] == "timestamp(s)" and col == "Source extraction date":
                    df[col] = pd.to_datetime(df[col],format=col_metadata["datetime_format"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
                elif col_metadata["type"] == "timestamp(s)" and col == "Date of birth":
                    df[col] = (df[col] + "T00:00:00")
                    pd.to_datetime(df[col],format=col_metadata["datetime_format"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
                else:
                    df[col] = df[col].astype(col_metadata["type"])
    return df

def add_mojap_columns_to_dataframe(df):
    # Add entries to metadata
    metadata_path = "data/metadata/intro-project-metadata.json"
    metadata = Metadata.from_json(metadata_path)
    metadata.update_column({"name": "mojap_start_datetime", "type": "timestamp(s)"})
    metadata.update_column({"name": "mojap_image_tag", "type": "string"})
    metadata.update_column({"name": "mojap_raw_filename", "type": "string"})
    metadata.update_column({"name": "mojap_task_timestamp", "type": "timestamp(s)"})

    # Add columns to dataframe
    df["mojap_start_datetime"] = pd.to_datetime(df["source_extraction_date"])
    df["mojap_image_tag"] = globals().get("AIRFLOW_IMAGE_TAG", "")
    df["mojap_raw_filename"] = globals().get("RAW_FILENAME", "")
    df["mojap_task_timestamp"] = pd.to_datetime(globals().get("AIRFLOW_TASK_TIMESTAMP", ""))
    return df


def write_curated_table_to_s3(df):
    # Write DataFrame to Parquet format using Arrow PD Parser
    wr.s3.to_parquet(df, path=s3_path, dataset=True, index=False)
    columns_types = {col: str(df[col].dtype) for col in df.columns}

    # Optionally, register the table in the Glue Catalog
    wr.catalog.create_parquet_table(
        database="default",
        table="curated_table",
        path=s3_path,
        columns_types=columns_types,
        #partition_cols=[],  # If you have partition columns, specify them here
    )
    


def move_completed_files_to_raw_hist():
    # Get the source bucket and folder path from environment variables
    source_bucket = os.getenv('S3_BUCKET_NAME')
    source_folder_path = os.getenv('S3_FOLDER_PATH')

    # Create an S3 client
    s3_client = boto3.client('s3')

    # Specify the source key as the source folder path
    source_key = source_folder_path

    # Check if the destination folder exists, if not, create it
    destination_folder = 'raw_hist/'
    destination_key = destination_folder.lstrip('/')
    try:
        s3_client.head_object(Bucket=source_bucket, Key=destination_folder)
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.put_object(Bucket=source_bucket, Key=destination_folder, Body='')
        else:
            raise

    # Copy the object from the source location to the destination location
    s3_client.copy_object(
        Bucket=source_bucket,
        Key=destination_key,
        CopySource={'Bucket': source_bucket, 'Key': source_key}
    )

    # Delete the object from the source location
    # s3_client.delete_object(
    #     Bucket=source_bucket,
    #     Key=source_key
    # )

def apply_scd2():
    """
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
