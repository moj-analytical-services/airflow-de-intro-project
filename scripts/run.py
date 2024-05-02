from .functions_ import *

def run_data_pipeline():
    extract_data_to_s3()
    df = load_data_from_s3()
    df = cast_columns_to_correct_types(df)
    df = add_mojap_columns_to_dataframe(df)
    write_curated_table_to_s3(df)
    move_completed_files_to_raw_hist()
