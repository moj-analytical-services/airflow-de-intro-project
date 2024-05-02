import os
import shutil
from datetime import datetime

import pyarrow.parquet as pq

import pandas as pd
import awswrangler as wr
from mojap_metadata import Metadata
from arrow_pd_parser import reader
from typing import  Dict, Union, Optional

# from config import settings








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

def load_partitioned_parquets(s3_path, partitions=None) -> pd.DataFrame:
    df = []
    
    dataset = pq.ParquetDataset(s3_path, use_legacy_dataset=False)
    
    for frag in dataset.fragments:
        if 'part' in frag.path:
            if partitions and any(partition not in frag.path for partition in partitions):
                continue
            
            df_frag = reader.parquet.read(frag.path)
            df.append(df_frag)
    
    full_df = pd.concat(df, ignore_index=True)
    
    return full_df


import boto3
import pyarrow.parquet as pq
import pandas as pd

def load_partitioned_parquetss(s3_path, partitions=None):
    s3 = boto3.resource('s3')
    bucket_name, prefix = s3_path.replace('s3://', '').split('/', 1)
    bucket = s3.Bucket(bucket_name)

    dfs = []
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith('.parquet'):
            if partitions and not any(partition in obj.key for partition in partitions):
                continue

            parquet_file = 's3://{}/{}'.format(bucket_name, obj.key)
            df_frag = pq.ParquetDataset(parquet_file, use_legacy_dataset=False).read().to_pandas()
            dfs.append(df_frag)

    full_df = pd.concat(dfs, ignore_index=True)
    return full_df


import s3fs
import pyarrow.parquet as pq
from arrow_pd_parser import reader

s3 = s3fs.S3FileSystem()
def load_partitioned_parquets(s3_path, partitions=None):
    """
    Load Parquet files iteratively from an S3 bucket when the file names contain 'part'.
    
    Args:
        s3_path (str): The S3 path to the directory containing the Parquet files.
        partitions (list, optional): A list of partition values to filter files.
    
    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the combined data from all files.
    """
    # Create an empty list to store the data
    data_frames = []
    
    # Create a ParquetDataset from the S3 path
    dataset = pq.ParquetDataset(s3_path, filesystem=s3)
    
    # Iterate over the Parquet file pieces
    for piece in dataset.fragments:
        # Check if the file path contains 'part'
        if 'part' in piece.path:
            # If partitions are provided, filter files based on partition values
            if partitions and any(partition not in piece.path for partition in partitions):
                continue
            
            # Load the Parquet file into a DataFrame
            df = reader.parquet.read(piece.path)
            data_frames.append(df)
    
    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(data_frames, ignore_index=True)
    
    return combined_df


def load_data_from_s3(
                LOCAL=True, partitions: Optional[str] = None
            ) -> pd.DataFrame:
    if LOCAL:
        file_path = os.path.join(
            os.getcwd(), "data/example-data"
        )
    else:
        file_path = (
            "s3://dami-test-bucket786567/de-intro/land/"
        )
    df = []
    dataset = pq.ParquetDataset(
        file_path, use_legacy_dataset=False
    )

    for frag in dataset.fragments:
        if partitions and any(
            partition not in frag.path
            for partition in partitions
        ):
            continue
        df_frag = reader.parquet.read(frag.path)
        df.append(df_frag)
    full_df = pd.concat(df, ignore_index=True)

    return full_df

# Example usage
url = 's3://dami-test-bucket786567/de-intro/land/'
df = load_partitioned_parquetss(url)

#df = pq.ParquetDataset(url, filesystem=s3)
print(df)




import boto3
from botocore.exceptions import ClientError

def write_curated_table_to_s3(
    df: pd.DataFrame, overwrite_table: Optional[bool] = True
):

    # Parameters
    db_dict: Dict[str, Union[str, None]] = {
        "name": "dami_intro_project", # database name
        "description": "database with data from people parquet",
        "table_name": "peoples", # table name
        "table_location": settings.CURATED_FOLDER #"s3://dami-test-bucket786567/de-intro/",
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
        if (
            e.response["Error"]["Code"]
            == "EntityNotFoundException"
        ):
            glue_client.create_database(**db_meta)
        else:
            print(f"Unexpected error: {e}")

    # Create Table
    try:
        table_exists = True
        glue_client.get_table(
            DatabaseName=db_dict["name"],
            Name=db_dict["table_name"],
        )
    except ClientError as e:
        if (
            e.response["Error"]["Code"]
            == "EntityNotFoundException"
        ):
            table_exists = False
        else:
            print(f"Unexpected error: {e}")
    if table_exists:
        if overwrite_table:
            print(
                f"Deleting existing table - '{db_dict['table_name']}' in database - '{db_dict['name']}'"
            )

            wr.catalog.delete_table_if_exists(
                database=db_dict["name"],
                table=db_dict["table_name"],
            )
            spec = gc.generate_from_meta(
                metadata,
                database_name=db_dict["name"],
                table_location=db_dict["table_location"],
            )
            glue_client.create_table(**spec)

            print(
                f"Table - '{db_dict['table_name']}' created in database - '{db_dict['name']}'"
            )
        else:
            print(
                f"Table {db_dict['table_name']} already exists in database {db_dict['name']}"
            )
    else:
        spec = gc.generate_from_meta(
            metadata,
            database_name=db_dict["name"],
            table_location=db_dict["table_location"],
        )
        glue_client.create_table(**spec)
        print(
            f"Table {db_dict['table_name']} created in database {db_dict['name']}"
        )
    print(
        f"writing Data to Table {db_dict['table_name']} in database {db_dict['name']}"
    )
    file_path = s3_path_join(
        db_dict["table_location"],
        f"{db_dict['table_name']}.parquet",  # {time.time_ns()}
    )

    # Write the parquet file
    writer.write(
        df=df,
        output_path=file_path,
        metadata=metadata,
        file_format="parquet",
    )

    # Register the table with the Glue Catalog
    wr.catalog.create_parquet_table(
        database=db_dict["name"],
        table=db_dict["table_name"],
        path=db_dict["table_location"],
        columns_types=athena_columns(metadata),
        description="Curated people data",
    )



write_curated_table_to_s3()
