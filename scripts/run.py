import os
import tempfile
from dataengineeringutils3.s3 import (
    copy_s3_object,
    write_local_file_to_s3,
)

run = os.getenv("RUN")
from_path = os.getenv("FROM")
to_path = os.getenv("TO")
text = os.getenv("TEXT")
write_outpath = os.getenv("OUTPATH")

if run == "copy":
    if not from_path or not to_path:
        raise ValueError(f"Missing FROM and/or TO env vars. Got FROM => {from_path}. TO => {to_path}.")
    copy_s3_object(from_path, to_path)

elif run == "write":
    if not text or not write_outpath:
        raise ValueError(f"Missing TEXT and/or OUTPATH env vars. Got TEXT => {text}. OUTPATH => {write_outpath}.")
    with open("tmp.txt", "w") as f:
        f.write(text)
    write_local_file_to_s3("tmp.txt", write_outpath, overwrite=True)
else:
    raise ValueError(f"Bad RUN env var. Got {run}. Expected 'copy' or 'write'.")
