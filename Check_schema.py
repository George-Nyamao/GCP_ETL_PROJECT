import pandas as pd
from google.cloud import storage
import os

# Set up your Google Cloud project ID and credentials
project_id = 'gcap-data-pipeline'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/gmnya/Documents/2ndStorage/GCP_ETL_Project/gcap-data-pipeline-f6531dcc54ac.json'

# Initialize Google Cloud Storage client
storage_client = storage.Client(project=project_id)

def download_csv_from_gcs(bucket_name, source_blob_name, destination_file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"File {source_blob_name} downloaded from bucket {bucket_name} to {destination_file_name}.")

def check_csv_schema(file_path):
    df = pd.read_csv(file_path)
    print("Schema (Column Names):")
    print(df.columns)
    print("\nData Types:")
    print(df.dtypes)
    print("\nFirst Few Rows:")
    print(df)

# Set your GCS bucket name and file details
bucket_name = 'bkt-morara-employee-data'
source_blob_name = 'employee.csv'
destination_file_name = 'employee_local_file.csv'

# Download the file from GCS
# download_csv_from_gcs(bucket_name, source_blob_name, destination_file_name)

# Check the schema of the downloaded CSV file
check_csv_schema(destination_file_name)
