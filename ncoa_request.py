import uuid
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from config import BIGQUERY_SERVICE_ACCOUNT_FILE

# Initialize BigQuery client
client = bigquery.Client.from_service_account_json(BIGQUERY_SERVICE_ACCOUNT_FILE)

# Generate a unique job_id and current datetime
job_id = str(uuid.uuid4())
job_run_datetime = datetime.utcnow()


# Query the vf_nc_partial table
source_table = "vr-mail-generator.voterfile.vf_nc_partial"
query_limit = None
query = f"""
SELECT
    vr_program_id AS individual_id,
    first_name AS individual_first_name,
    last_name AS individual_last_name,
    mail_addr1 AS address_line_1,
    mail_addr2 AS address_line_2,
    mail_city AS address_city_name,
    'NC' AS address_state_code,
    mail_zipcode AS address_postal_code
FROM `{source_table}`
WHERE vr_program_id IS NOT NULL
"""
if query_limit is not None:
    query += f"LIMIT {query_limit}"
query_job = client.query(query)
results = query_job.result()

# Convert query results to a DataFrame
data = [
    {
        "individual_id": row.individual_id,
        "individual_first_name": row.individual_first_name,
        "individual_last_name": row.individual_last_name,
        "address_line_1": row.address_line_1,
        "address_line_2": row.address_line_2 or "",
        "address_city_name": row.address_city_name,
        "address_state_code": row.address_state_code,
        "address_postal_code": row.address_postal_code,
    }
    for row in results
]
df = pd.DataFrame(data)

# Save DataFrame to CSV
csv_filename = f"job_request_input_id_{job_id}.csv"
df.to_csv(csv_filename, index=False)


def query_NCOA_data(test=False):
    pass
