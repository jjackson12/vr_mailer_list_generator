import pandas as pd
import os
from google.cloud import bigquery

# Set path to your local BigQuery credentials JSON
credentials_path = "/Users/jacobjackson/Dev/2025_projects/vr_mail_list_generator/vr-mail-generator-8e97a63564fe.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Initialize BigQuery client
client = bigquery.Client()

file_path = "scripts/nc_vf_full.csv"

df = pd.read_csv(file_path, dtype=str, low_memory=False)

# Define table ID
table_id = "vr-mail-generator.voterfile.vf_nc_full"

# Upload DataFrame to BigQuery
job = client.load_table_from_dataframe(df, table_id)
job.result()  # Wait for the job to complete
