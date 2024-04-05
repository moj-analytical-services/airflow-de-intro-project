# Functions_.py - 

In the `load_data_from_s3` function, we use the reader.read method from the arrow_pd_parser library to read the CSV file from the specified S3 path. This method returns a Pandas DataFrame.

In the `cast_columns_to_correct_types` function:
- We first load the metadata schema from a JSON file using mojap_metadata.Metadata.from_file.
- We iterate over the columns in the metadata schema.
- For each column, we get the column name and the expected data type from the metadata.
- We use the astype method of the Pandas DataFrame to cast the column to the correct data type using the mojap_metadata.types.python_type function, which converts the metadata type to the corresponding Python data type.

In the `add_mojap_columns_to_dataframe` function:
- We load the metadata schema from a JSON file using mojap_metadata.Metadata.from_file.
- We add new columns to the metadata using the update_column method.
- We add the new columns to the DataFrame:
    - mojap_start_datetime is derived from the "Source extraction date" column.
    - mojap_image_tag is retrieved from an environment variable (AIRFLOW_IMAGE_TAG).
    - mojap_raw_filename is set to the source file name ("people-100000.csv").
    - mojap_task_timestamp is retrieved from an environment variable (AIRFLOW_TASK_TIMESTAMP) or set to the current datetime if the variable is not available.

In the `write_curated_table_to_s3` function:
- We convert the metadata to a Glue schema using mojap_metadata.GlueConverter.
- We use wr.s3.to_parquet to write the DataFrame to S3 in Parquet format, creating a dataset with the specified database, table, partition columns, and metadata.
- We use wr.catalog.create_parquet_table to register the table with the Glue Catalog, providing the schema, partition columns, and metadata.


The `move_completed_files_to_raw_hist` function performs the following steps in a loop, iterating over files in the Land folder:
- It utilizes the `file_processing_completed` function to determine if the file has finished processing.
- If the file has completed processing, it employs `shutil.move` to transfer the file from the Land folder to the Raw_Hist folder.

The `file_processing_completed` function is a placeholder where you need to add your logic to determine if a file has completed processing. In the example, it checks for the existence of a flag file with the same name as the original file and a .processed extension. You can modify this function to suit your specific requirements.

The `apply_scd2` function is designed to perform the following operations:
- Load the three data files (people-part1.parquet, people-part2.parquet, and people-part3.parquet) using pd.read_parquet. The data frames are stored in memory.

- Concatenate the data frames together and sort them based on two columns, user_id and mojap_start_datetime.

- Invoke the `apply_scd2_logic` function, passing in the sorted data frame. Inside this function, the SCD2 (Slowly Changing Dimension Type 2) logic is applied to the data frame. This involves adding a `row_id` column to uniquely identify each row and an `is_current` flag column to determine the current row for each user, based on the `mojap_start_datetime`.

- Save the resulting SCD2 data frame to a new file named people_scd2.parquet.

It's important to note that you should update the file paths in the code snippet with the appropriate paths for your specific scenario. Additionally, you might need to modify the `apply_scd2_logic` function to accommodate any specific requirements or transformations necessary for your data.

---