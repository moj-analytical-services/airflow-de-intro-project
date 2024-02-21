
def load_data_from_s3():
    """
    In this stage we need to load our dataset from an s3 bucket
    and return it to the rest of the pipeline in the form of
    a Pandas DataFrame.
    To do this we will utilise the read() method of an arrow_pd_parser 
    reader object, this method will require the path to the data
    object on s3
    Arrow PD Parser: 
    https://github.com/moj-analytical-services/mojap-arrow-pd-parser
    
    """
    return df

def cast_columns_to_correct_types(df):
    """
    In this stage we need to compare the data types of each column in our
    pandas dataframe and ensure they match with the expected
    types from the provided Mojap Metadata, casting types as required.
    Mojap Metadata:
    https://github.com/moj-analytical-services/mojap-metadata
    """
    return df

def add_mojap_columns_to_dataframe(df):
    """
    In this stage we need to add a set of columns to the dataframe and
    metadata which are derived from environment variables, this will allow
    traceability for when entries were added, which version of the pipeline
    was used.
    To add entries to the Metadata we utilise the update_column() method of
    the metadata object.
    Once this is done, columns should be added to the dataframe ensuring the
    correct data type is used.
    """
    return df

def write_data_to_s3(df):
    """
    Once all transformations on the dataframe are completed we need to write
    the data to an appropriate s3 bucket written in .parquet format.
    An Arrow PD Parser writer object can be used to acheive this.
    The table should then be registered with the Glue Catalogue.
    AWS Wrangler can be used to acheive this:
    https://github.com/aws/aws-sdk-pandas
    specifically the create_parquet_table method of the Catalogue module.
    Before this, we will need to convert the metadata to an appropriate format using
    a GlueConverter object from Mojap Metadata
    """
    return

