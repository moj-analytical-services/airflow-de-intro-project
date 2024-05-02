"""Parses and performs initial validation of the settings based on
environment variables (in production these are set via the Airflow DAG)"""
import os
from typing import List, Optional, Union

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings

import re

LOCAL_DEV_MODE = True
DEFAULT_SECRET_PREFIX = "/alpha/airflow/airflow_prod_laa/"


class Settings(BaseSettings):
    """Tries to read the value for each of these fields from the env var
    with the same name, then if specified converts it to that type. If
    there is no env var with that name set, applies the default value. If
    not set and no default value specified, throws an error (these are not
    optional and are required to be set via the DAG)"""
    AWS_REGION: str = "eu-west-2"
    MOJAP_EXTRACTION_TS: int
    MOJAP_IMAGE_VERSION: str
    TABLE_PREFIX: Optional[str] = None
    TABLES: Optional[str] = None #ßOptional[Union[str, List[str]]] = None

    LANDING_FOLDER: Optional[str] = None
    RAW_HIST_FOLDER: Optional[str] = None
    CURATED_FOLDER: Optional[str] = None
    METADATA_FOLDER: Optional[str] = None
    # Name of the secret in the AWS Parameter Store
    SECRET_NAME_USER: str = f"{DEFAULT_SECRET_PREFIX}user-name"
    SECRET_NAME_PASSWORD: str = f"{DEFAULT_SECRET_PREFIX}password"

    @model_validator(mode="before")
    def check_land_and_or_meta(cls, values):
        """At least one of LANDING_FOLDER and METADATA_FOLDER must be
        set"""
        if (values.get("LANDING_FOLDER") is None) and (
            values.get("METADATA_FOLDER") is None
        ):
            raise ValueError(
                "At least one of LANDING_FOLDER or METADATA_FOLDER is required"
            )
        return values

    # @model_validator(mode="before")
    # def check_prefix_or_tables(cls, values):
    #     """One and only one of TABLE_PREFIX and TABLES must be set"""
    #     if (values.get("TABLE_PREFIX") is None) and (values.get("TABLES") is None):
    #         raise ValueError("One of TABLE_PREFIX or TABLES is required")
    #     if (values.get("TABLE_PREFIX") is not None) and (
    #         values.get("TABLES") is not None
    #     ):
    #         raise ValueError("One and only one of TABLE_PREFIX and TABLES must be set")
    #     return values

    @classmethod
    def string_match(cls, strg: str,
                     match=re.compile(r'[A-Z0-9]+(_$)').match) -> bool:
        """
        Returns false if strg is NOT stringType
        AND
        Returns True only if pattern consists of Uppercase Alphanumerics
        followed by an Underscore.
        """
        if not isinstance(strg, str):
            return False
        return bool(match(strg))


print("Instantiating settings ...")
if LOCAL_DEV_MODE:
    settings = Settings(_env_file="dev.env")
else:
    settings = Settings()
    
os.environ["AWS_REGION"] = settings.AWS_REGION
os.environ["AWS_DEFAULT_REGION"] = settings.AWS_REGION