import awswrangler as wr
from mojap_metadata import Metadata
from typing import Dict, Union, Optional
from urllib.parse import urljoin, urlparse, urlunparse
from mojap_metadata.converters.glue_converter import GlueConverter


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


def get_table_location(database_name: str, table_name: str):
    # Get list of tables in the specified database
    table_info = list(wr.catalog.get_tables(database=database_name))
    
    # Extract table names
    table_names = [x["Name"] for x in table_info]

    if table_name in table_names:
        # Get information about the specified table
        table_info = next(x for x in table_info if x["Name"] == table_name)
        
        # Extract location of the table
        table_location = table_info["StorageDescriptor"]["Location"]
        
        return table_location
    else:
        raise ValueError(f"Table '{table_name}' not found in database '{database_name}'")
    

