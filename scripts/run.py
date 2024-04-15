# Nothing has been changed just want to create a image
import os
from dataengineeringutils3.s3 import (
    write_local_folder_to_s3,
)

run = os.getenv("RUN")
file_to_write = os.getenv("DATA_PATH")
write_outpath = os.getenv("OUTPATH")

if run == "write":
    write_local_folder_to_s3(file_to_write, write_outpath, overwrite=True)
else:
    raise ValueError(f"Bad RUN env var. Got {run}. Expected 'write'.")
