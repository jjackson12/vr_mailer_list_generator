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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from google.cloud import storage
from typing import List, Dict, Any
import re
import logging
from google.cloud import bigquery
import zipfile
import os
import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# CONFIG
BUCKET_NAME = "vr_mail_lists"
REVIEWER_EMAIL = "jjjackson116@gmail.com"
BUCKETS_SERVICE_ACCOUNT_KEY = "vr-mail-generator-56bee8a8278b.json"
BIGQUERY_SERVICE_ACCOUNT_KEY = "vr-mail-generator-8e97a63564fe.json"


# # SMTP Configuration
# SMTP_SERVER = "smtp.mailersend.net"  # Replace with actual SMTP server
# SMTP_PORT = 2525  # 465 for SSL, 587 for TLS
# SMTP_USERNAME = "MS_YLCxnn@test-vz9dlem08m64kj50.mlsender.net"
# SMTP_PASSWORD = "mssp.gFBRElb.351ndgw9jyr4zqx8.yu20BC7"

with open("mailsend_access_token.txt", "r") as file:
    MAILSEND_ACCESS_TOKEN = file.read().strip()


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
        # Remove parameters with empty lists or None values
        params = {k: v for k, v in params.items() if v not in (None, [])}

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
        int_fields = [
            "StateSenateDistrict",
            "StateHouseDistrict",
            "CongressionalDistrict",
        ]
        # Add filters dynamically based on params
        for key, value in params.items():
            if value is not None:
                param_db_name = RENAME_MAP[key]
                if key in special_queries:
                    query += special_queries[key](value)
                else:
                    if isinstance(value, list):
                        if key in int_fields:
                            value_list = ", ".join([str(int(v)) for v in value])
                        else:
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
    ):
        """Create a random or stratified control group and return both control and treatment groups."""
        # TODO: Test stratification. Also consider when we'd want to oversample on subgroups, but this would require adjusting the end result analysis as well, so that's a more complex workflow I don't want to try to account for in this yet
        if stratify:
            logger.info(f"Creating stratified control group on {stratify}")
            # Stratified sampling (naive)
            control_group = df.groupby(stratify, group_keys=False).apply(
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
            control_group = df.sample(frac=frac)

        logger.info(f"Control group rows={len(control_group)} (source rows={len(df)})")

        # Treatment group is the complement of the control group
        treatment_group = df.drop(control_group.index)
        logger.info(f"Treatment group rows={len(treatment_group)}")

        return control_group, treatment_group

    def upload_to_gcs(self, file_path: str, bucket_name: str, dest_blob_name: str):
        """Upload file to Google Cloud Storage bucket."""
        logger.info(f"Uploading {file_path} to gs://{bucket_name}/{dest_blob_name}")
        bucket = self.buckets_client.bucket(bucket_name)
        blob = bucket.blob(dest_blob_name)
        blob.upload_from_filename(file_path)
        logger.info(f"Uploaded {file_path} -> gs://{bucket_name}/{dest_blob_name}")

    def email_completed_list(self, to_emails: List[str], list_name: str):
        subject = "Mailing List Completed: " + list_name
        # Attach the zip file to the email
        self.send_email(
            subject=subject,
            body=f"The mailing list '{list_name}' can now be downloaded from the Mailer generator app.",
            to_emails=to_emails,
        )

    def send_email(
        self, subject: str, body: str, to_emails: List[str], attachments_filepaths=None
    ):
        """Send an email notification using MailerSend API."""

        url = "https://api.mailersend.com/v1/email"
        headers = {
            "Authorization": f"Bearer {MAILSEND_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        }

        # Prepare the email data
        email_data = {
            "from": {
                "email": "jake.j3.jackson@gmail.com",
                "name": "VR Mail List Generator",
            },
            "to": [{"email": email} for email in to_emails],
            "subject": subject,
            "text": body,
        }

        # Add attachments if provided
        if attachments_filepaths:
            email_data["attachments"] = []
            for attachment_fp in attachments_filepaths:
                with open(attachment_fp, "rb") as attachment:
                    encoded_file = encoders.encode_base64(attachment.read())
                    email_data["attachments"].append(
                        {
                            "content": encoded_file.decode("utf-8"),
                            "filename": os.path.basename(attachment_fp),
                        }
                    )

        # # Send the email via MailerSend API
        # response = requests.post(url, headers=headers, json=email_data)

        # if response.status_code == 202:
        #     logger.info(f"Email sent successfully to {to_emails}: {subject}")
        # else:
        #     logger.error(
        #         f"Failed to send email. Status code: {response.status_code}, Response: {response.text}"
        #     )
        #     raise Exception(f"Email sending failed: {response.text}")

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
    def generate_rct_mailing_list(
        self,
        list_df: pd.DataFrame,
        requestor_email: str,
        requestor_name: str,
        request_name: str,
        params=None,
    ):
        logger.info(f"Request started for list size {len(list_df)}")
        if params:
            logger.info(f"Request parameters: {params}")
        # Mapping raw file columns -> friendly names used in params
        # Filter by registration address fields

        # New: derive unique cleaned request name
        cleaned_base = self.clean_request_name(request_name)
        logger.info(
            f"Cleaned request name='{cleaned_base}' (original='{request_name}')"
        )
        bucket = self.buckets_client.bucket(BUCKET_NAME)
        unique_name = self.ensure_unique_request_name(bucket, cleaned_base)
        logger.info(f"Resolved unique request name='{unique_name}'")
        gcs_base_path = f"lists/{unique_name}"

        list_df["Name"] = (
            list_df["first_name"].str.strip() + " " + list_df["last_name"].str.strip()
        )
        list_df["MailingAddress"] = (
            list_df["mail_addr1"].str.strip()
            + " "
            + list_df["mail_addr2"].fillna("").str.strip()
        )
        list_df["MailingCity"] = list_df["mail_city"].str.strip()
        list_df["MailingState"] = list_df["mail_state"].str.strip()
        list_df["MailingZip"] = list_df["mail_zipcode"].astype(str).str.strip()

        # Send emails (include original and final names)
        self.send_email(
            subject=f"Mailer List Request Received: {request_name}",
            body=(
                f"Hi {requestor_name}, we received your request named: '{request_name}'.\n"
            ),
            to_emails=[requestor_email, REVIEWER_EMAIL],
        )

        # Get mailing addresses for output
        list_df = list_df[
            ["Name", "MailingAddress", "MailingCity", "MailingState", "MailingZip"]
        ]
        logger.info(f"Total target group rows={len(list_df)}")
        # Create control group
        control_group, treatment_group = self.create_control_group(list_df)
        logger.info(f"Control mailing rows={len(control_group)}")
        logger.info(f"Treatment mailing rows={len(treatment_group)}")
        # Group treatment list by household
        grouped_treatment_mailing = (
            treatment_group.groupby(
                ["MailingAddress", "MailingCity", "MailingState", "MailingZip"]
            )
            .agg({"Name": lambda x: "Household of " + " and ".join(x)})
            .reset_index()
        )

        # Group control list by household
        grouped_control_mailing = (
            control_group.groupby(
                ["MailingAddress", "MailingCity", "MailingState", "MailingZip"]
            )
            .agg({"Name": lambda x: "Household of " + " and ".join(x)})
            .reset_index()
        )

        # Upload PEOPLE lists to GCS
        bucket.blob(f"{gcs_base_path}/treatment_group.csv").upload_from_string(
            treatment_group.to_csv(index=False), content_type="text/csv"
        )
        bucket.blob(f"{gcs_base_path}/control_group.csv").upload_from_string(
            control_group.to_csv(index=False), content_type="text/csv"
        )
        logger.info("PEOPLE uploads to GCS complete")

        # Upload grouped MAILING lists to GCS
        bucket.blob(f"{gcs_base_path}/treatment_mailing_list.csv").upload_from_string(
            grouped_treatment_mailing.to_csv(index=False), content_type="text/csv"
        )
        bucket.blob(f"{gcs_base_path}/control_mailing_list.csv").upload_from_string(
            grouped_control_mailing.to_csv(index=False), content_type="text/csv"
        )
        logger.info("MAILING uploads to GCS complete")

        final_name = unique_name

        self.email_completed_list(
            to_emails=[requestor_email, REVIEWER_EMAIL], list_name=final_name
        )
        logger.info("Notification emails processed")
        logger.info("Request completed successfully")


# if __name__ == "__main__":
#     # Example usage
#     params = {
#         "County": [],
#         "Party": [],
#         "Race": [],
#         "Ethnicity": [],
#         "Gender": [],
#         "Age": [18, 100],
#         "StateHouseDistrict": [],
#         "StateSenateDistrict": [],
#         "CongressionalDistrict": [],
#     }

#     generator = VRMailListGenerator()
#     target_voters = generator.filter_voters(params)
#     generator.generate_rct_mailing_list(
#         list_df=target_voters,
#         requestor_email="user@example.com",
#         requestor_name="Jane Doe",
#         request_name="Test Request: Female Dems Under 50",
#     )
