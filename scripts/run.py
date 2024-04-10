import os
from dataengineeringutils3.s3 import (
    write_local_folder_to_s3,
)
from functions_ import *

df = load_data_from_s3()
df = cast_columns_to_correct_types(df)
df = add_mojap_columns_to_dataframe(df)
print(df)

write_curated_table_to_s3(df)



# move_completed_files_to_raw_hist()

# run = os.getenv("RUN")
# file_to_write = os.getenv("PATH")
# write_outpath = os.getenv("OUTPATH")

# if run == "write":
#     write_local_folder_to_s3(file_to_write, write_outpath, overwrite=True)
# else:
#     raise ValueError(f"Bad RUN env var. Got {run}. Expected 'write'.")