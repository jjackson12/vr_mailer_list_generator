import os
import zipfile
import tempfile
import pandas as pd
import requests
from google.cloud import bigquery


election_results_url = (
    "https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2024_11_05/results_pct_20241105.zip"
)

vf_url = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip"

vote_history_url = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis_Statewide.zip"

# Set path to your local BigQuery credentials JSON
credentials_path = "/Users/jacobjackson/Dev/2025_projects/vr_mail_list_generator/vr-mail-generator-8e97a63564fe.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Initialize BigQuery client
client = bigquery.Client()


def download_and_extract_txt(url):
    # Determine local zip and txt file paths
    zip_filename = os.path.basename(url)
    zip_local_path = os.path.join(os.path.dirname(__file__), zip_filename)
    txt_filename = None

    # Download zip if not present locally
    if not os.path.exists(zip_local_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(zip_local_path, "wb") as f:
            f.write(response.content)

    # Extract .txt file if not present locally
    with zipfile.ZipFile(zip_local_path, "r") as zip_ref:
        txt_files = [f for f in zip_ref.namelist() if f.endswith(".txt")]
        if not txt_files:
            raise ValueError("No .txt file found in zip")
        txt_filename = txt_files[0]
        txt_local_path = os.path.join(os.path.dirname(__file__), txt_filename)
        if not os.path.exists(txt_local_path):
            zip_ref.extract(txt_filename, os.path.dirname(__file__))
    return txt_local_path, txt_filename


def upload_to_bigquery(txt_path, txt_file):
    df = pd.read_csv(txt_path, sep="\t", dtype=str)
    if "ncvoter" in txt_file.lower():
        table_id = f"vr-mail-generator.voterfile.vf_nc_full"
    else:
        table_id = f"vr-mail-generator.voterfile.{os.path.splitext(os.path.basename(txt_file))[0]}"
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()


for url in [vf_url, vote_history_url]:  # [election_results_url]:
    txt_path, txt_file = download_and_extract_txt(url)
    upload_to_bigquery(txt_path, txt_file)
