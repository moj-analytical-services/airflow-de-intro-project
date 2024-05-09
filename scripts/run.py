# Nothing has been changed just want to create a image
# import os
# from dataengineeringutils3.s3 import (
#     write_local_folder_to_s3,
# )

# run = os.getenv("RUN")
# file_to_write = os.getenv("DATA_PATH")
# write_outpath = os.getenv("OUTPATH")

# if run == "write":
#     write_local_folder_to_s3(file_to_write, write_outpath, overwrite=True)
# else:
#     raise ValueError(f"Bad RUN env var. Got {run}. Expected 'write'.")
import temp_function

def main():
    # Load files from S3
    df = temp_function.load_files_from_s3()
    
    # Cast columns to correct types
    df = temp_function.cast_columns_to_correct_types(df)
    
    # Add custom columns to DataFrame
    df = temp_function.add_mojap_columns_to_dataframe(df)
    
    # Write curated table to S3
    temp_function.write_curated_table_to_s3(df)
    
    # Move completed files to raw_hist in S3
    temp_function.move_completed_files_to_raw_hist()

if __name__ == "__main__":
    main()