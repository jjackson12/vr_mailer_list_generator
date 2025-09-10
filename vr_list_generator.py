"""
Script to generate a mailing list based on search parameters, create a control group, upload to Google Cloud Storage, and send email notifications.

Requirements:
- pandas
- google-cloud-storage
- smtplib (or other email lib)

Assumes voter registration & mailing data in 'nc_vf_partial.csv'.
"""

import pandas as pd
import random
import smtplib
from email.mime.text import MIMEText
from google.cloud import storage
from typing import List, Dict, Any
import re
import logging
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# CONFIG
DATA_PATH = "scripts/nc_vf_partial.csv"
BUCKET_NAME = "vr_mail_lists"
REVIEWER_EMAIL = "jake.j3.jackson@gmail.com"
BUCKETS_SERVICE_ACCOUNT_KEY = "vr-mail-generator-56bee8a8278b.json"
BIGQUERY_SERVICE_ACCOUNT_KEY = "vr-mail-generator-8e97a63564fe.json"


class VRMailListGenerator:

    def __init__(self):
        self.bq_client = bigquery.Client.from_service_account_json(
            BIGQUERY_SERVICE_ACCOUNT_KEY
        )
        self.buckets_client = storage.Client.from_service_account_json(
            BUCKETS_SERVICE_ACCOUNT_KEY
        )

    def filter_voters(self, params: Dict[str, Any]) -> pd.DataFrame:
        """Filter voters by querying BigQuery based on search parameters."""
        logger.info("Querying BigQuery for voter data")

        # Base query
        query = "SELECT * FROM `vr-mail-generator.voterfile.vf_nc_partial` WHERE 1=1"
        # Mutate columns to match parameters
        RENAME_MAP = {
            "Party": "party_cd",
            "Age": "age_at_year_end",
            "Gender": "gender_code",
            "Race": "race_code",
            "Ethnicity": "ethnic_code",
            "County": "county_desc",
            "CongressionalDistrict": "cong_dist_abbrv",
            "StateSenateDistrict": "nc_senate_abbrv",
            "StateHouseDistrict": "nc_house_abbrv",
        }
        special_queries = {
            "Age": lambda x: f" AND age_at_year_end BETWEEN {min(x)} AND {max(x)}"
        }
        # TODO: This logic needs to find its way back at the end of the generator step
        # df["Name"] = df["first_name"].str.strip() + " " + df["last_name"].str.strip()
        # df["MailingAddress"] = (
        #     df["mail_addr1"].str.strip() + " " + df["mail_addr2"].fillna("").str.strip()
        # )
        # df["MailingCity"] = df["mail_city"].str.strip()
        # df["MailingState"] = df["mail_state"].str.strip()
        # df["MailingZip"] = df["mail_zipcode"].astype(str).str.strip()

        # Add filters dynamically based on params
        for key, value in params.items():
            if value is not None:
                param_db_name = RENAME_MAP[key]
                if key in special_queries:
                    query += special_queries[key](value)
                else:
                    if isinstance(value, list):
                        value_list = ", ".join([f"'{v}'" for v in value])
                        query += f" AND {param_db_name} IN ({value_list})"
                    else:
                        query += f" AND {param_db_name} = '{value}'"

        logger.debug(f"Constructed query: {query}")

        # Execute query
        query_job = self.bq_client.query(query)
        result = query_job.result()
        df = result.to_dataframe()

        logger.info(f"Query complete: rows fetched={len(df)}")
        return df

    def create_control_group(
        self, df: pd.DataFrame, size: int = None, stratify: List[str] = None
    ) -> pd.DataFrame:
        """Create a random or stratified control group."""
        if stratify:
            logger.info(f"Creating stratified control group on {stratify}")
            # Stratified sampling (naive)
            return df.groupby(stratify, group_keys=False).apply(
                lambda x: x.sample(frac=0.1)
            )
        else:
            logger.info(
                "Creating random control group (10% default)"
                if size is None
                else f"Creating random control group size={size}"
            )
            # Truly random sample (10% default)
            frac = 0.1 if size is None else min(size / len(df), 1)
            result = df.sample(frac=frac)
            logger.info(f"Control group rows={len(result)} (source rows={len(df)})")
            return result

    def upload_to_gcs(self, file_path: str, bucket_name: str, dest_blob_name: str):
        """Upload file to Google Cloud Storage bucket."""
        logger.info(f"Uploading {file_path} to gs://{bucket_name}/{dest_blob_name}")
        bucket = self.buckets_client.bucket(bucket_name)
        blob = bucket.blob(dest_blob_name)
        blob.upload_from_filename(file_path)
        logger.info(f"Uploaded {file_path} -> gs://{bucket_name}/{dest_blob_name}")

    def send_email(self, subject: str, body: str, to_emails: List[str]):
        """Send an email notification."""
        # Example using SMTP (configure for your provider)
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = "mailer@example.com"
        msg["To"] = ", ".join(to_emails)
        logger.info(f"Queued email: subject='{subject}' to={to_emails}")
        # TODO: SMTP config here
        # with smtplib.SMTP('smtp.example.com') as server:
        #     server.login('user', 'pass')
        #     server.sendmail(msg['From'], to_emails, msg.as_string())
        print(f"Email sent to {to_emails}: {subject}")

    def clean_request_name(self, raw: str) -> str:
        """Produce a filesystem / GCS safe base name."""
        cleaned = raw.strip().lower()
        cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
        cleaned = re.sub(r"_+", "_", cleaned).strip("_")
        return cleaned or "request"

    def ensure_unique_request_name(self, bucket, base_name: str) -> str:
        """
        If any blob exists under lists/<base_name>/ treat as conflict.
        Append -1, -2, ... until unique.
        """
        candidate = base_name
        counter = 1
        while True:
            prefix = f"lists/{candidate}/"
            # List only one blob to detect existence quickly
            blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
            if not blobs:
                return candidate
            candidate = f"{base_name}-{counter}"
            counter += 1

    # --- Main Logic ---
    def main(
        self,
        params: Dict[str, Any],
        requestor_email: str,
        requestor_name: str,
        request_name: str,
    ):
        logger.info("Request started")
        logger.debug(f"Params received: {params}")
        # Mapping raw file columns -> friendly names used in params
        # Filter by registration address fields
        filtered = self.filter_voters(params)
        # Get mailing addresses for output
        mailing_list = filtered[
            ["Name", "MailingAddress", "MailingCity", "MailingState", "MailingZip"]
        ]
        logger.info(f"Mailing (treatment) rows={len(mailing_list)}")
        # Create control group
        control_group = self.create_control_group(filtered)
        control_mailing = control_group[
            ["Name", "MailingAddress", "MailingCity", "MailingState", "MailingZip"]
        ]
        logger.info(f"Control mailing rows={len(control_mailing)}")

        # New: derive unique cleaned request name
        cleaned_base = self.clean_request_name(request_name)
        logger.info(
            f"Cleaned request name='{cleaned_base}' (original='{request_name}')"
        )
        bucket = self.buckets_client.bucket(BUCKET_NAME)
        unique_name = self.ensure_unique_request_name(bucket, cleaned_base)
        logger.info(f"Resolved unique request name='{unique_name}'")
        gcs_base_path = f"lists/{unique_name}"

        # Local filenames include unique name
        treatment_local = f"{unique_name}/treatment_list.csv"
        control_local = f"{unique_name}/control_list.csv"
        logger.debug(f"Local output paths: {treatment_local}, {control_local}")

        # Save output
        mailing_list.to_csv(treatment_local.replace("/", "__"), index=False)
        control_mailing.to_csv(control_local.replace("/", "__"), index=False)
        logger.info("CSV files written locally")

        # Upload to GCS using required structure:
        # lists/<cleaned_list_name>/treatment_list.csv
        # lists/<cleaned_list_name>/control_list.csv
        bucket.blob(f"{gcs_base_path}/treatment_list.csv").upload_from_filename(
            treatment_local.replace("/", "__")
        )
        bucket.blob(f"{gcs_base_path}/control_list.csv").upload_from_filename(
            control_local.replace("/", "__")
        )
        logger.info("Uploads to GCS complete")

        original_name = request_name
        final_name = unique_name

        # Send emails (include original and final names)
        self.send_email(
            subject=f"Mailer List Request Received: {original_name}",
            body=(
                f"Hi {requestor_name}, we received your request named: '{original_name}'.\n"
                f"Final list identifier: {final_name}\n"
                f"Files will be stored under gs://{BUCKET_NAME}/{gcs_base_path}/"
            ),
            to_emails=[requestor_email, REVIEWER_EMAIL],
        )
        self.send_email(
            subject=f"Mailer List Ready: {final_name}",
            body=(
                f"Hi {requestor_name}, your mailing list (request: '{original_name}') is ready.\n"
                f"GCS paths:\n"
                f" - gs://{BUCKET_NAME}/{gcs_base_path}/treatment_list.csv\n"
                f" - gs://{BUCKET_NAME}/{gcs_base_path}/control_list.csv"
            ),
            to_emails=[requestor_email, REVIEWER_EMAIL],
        )
        logger.info("Notification emails processed")
        logger.info("Request completed successfully")


if __name__ == "__main__":
    # Example usage
    params = {
        "Party": "DEM",
        "Age": [25, 26, 27, 28, 29, 30],
        "Gender": "F",
    }
    generator = VRMailListGenerator()
    generator.main(
        params,
        requestor_email="user@example.com",
        requestor_name="Jane Doe",
        request_name="Spring Outreach Test",
    )
