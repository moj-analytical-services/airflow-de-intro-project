from arrow_pd_parser import reader, writer
import boto3

# Initialize a session using your credentials
#session = boto3.Session(
 #   aws_access_key_id='ASIAZ662XAMLMQR56HXF',
 #  aws_secret_access_key='1CQpU0a8UxAKLrEE4eD1OwHufwa01RYDmVKZX7ps',
 # region_name='eu-west-1'  
#)



df = reader.read("s3://alpha-mojap-ccd/ccd-analysis/airflow-de-intro-project/people-part1.parquet")
print(df.head())