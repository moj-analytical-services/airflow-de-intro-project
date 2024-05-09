# ARG DE_ECR 
FROM python:3.9-slim
# There are a number of other source images available:
# FROM ${DE_ECR}/python:3.9-bullseye
# FROM ${DE_ECR}/python:3.8-bullseye
# FROM ${DE_ECR}/python3.7-slim
# FROM ${DE_ECR}/python3.7
# FROM ${DE_ECR}/datascience-notebook:3.1.13
# FROM ${DE_ECR}/oraclelinux8-python:3.8

# Create a working directory to do stuff from
# Set environment variables
ENV S3_BUCKET_NAME "airflow-intro-test-murad"
ENV S3_FOLDER_PATH "loaded_data/"
WORKDIR /etl

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY scripts/ scripts/
COPY data/ data/
COPY docs/ docs/

# Ensures necessary permissions available to user in docker image
RUN chmod -R 777 .

ENTRYPOINT python -u scripts/run.py
