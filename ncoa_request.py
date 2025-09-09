import uuid
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from config import BIGQUERY_SERVICE_ACCOUNT_FILE


def query_NCOA_data(input_data, test=False):
    if test:
        # NOTE: This assumes no rows in our input data that do not exist in the sample test output data
        sample_output = pd.read_csv("sample_ncoa_output.csv")
        filtered_sample_output_data = input_data[
            input_data["individual_id"].isin(sample_output["record_id"])
        ]
        return filtered_sample_output_data
    else:
        # TODO: Implement actual NCOA query logic here
        return None


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
input_df = pd.DataFrame(data)

# Save DataFrame to CSV
# TODO: Save this to a google cloud bucket instead of locally
csv_filename = f"job_request_input_id_{job_id}.csv"
input_df.to_csv(csv_filename, index=False)
print(f"Input data saved to {csv_filename}")


ncoa_response = query_NCOA_data(input_df, test=True)

# TODO: Upload response to a google cloud bucket instead of locally
ncoa_response_csv_filename = f"job_request_response_id_{job_id}.csv"
ncoa_response.to_csv(ncoa_response_csv_filename, index=False)
print(f"NCOA response data saved to {ncoa_response_csv_filename}")

# Update the BigQuery table with the response data
table_id = "vr-mail-generator.vr_data.ncoa_address_statuses"

ncoa_response.rename(
    columns={
        "record_id": "vr_program_id",
        "first_name": "first_name",
        "last_name": "last_name",
        "address_line_1": "address_line_1",
        "city_name": "address_city",
        "state_code": "address_state",
        "postal_code": "address_zipcode",
        "address_status": "ncoa_status",
    },
    inplace=True,
)

# Create a temporary table in BigQuery to store the response data
temp_table_id = f"{table_id}_temp"
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    autodetect=True,  # <-- let BQ infer from df
)
client.load_table_from_dataframe(
    ncoa_response, temp_table_id, job_config=job_config
).result()

# Perform a single query to find matching and non-matching records
query = f"""
MERGE `{table_id}` AS target
USING `{temp_table_id}` AS source
ON target.vr_program_id = source.vr_program_id
   AND target.address_line_1 = source.address_line_1
   AND target.address_city = source.address_city
   AND target.address_zipcode = source.address_zipcode
   AND target.address_state = source.address_state
WHEN MATCHED THEN
  UPDATE SET
    last_attempted_update = CURRENT_TIMESTAMP(),
    ncoa_status = CASE
      WHEN target.ncoa_status != source.ncoa_status THEN source.ncoa_status
      ELSE target.ncoa_status
    END,
    last_updated = CASE
      WHEN target.ncoa_status != source.ncoa_status THEN CURRENT_TIMESTAMP()
      ELSE target.last_updated
    END
WHEN NOT MATCHED THEN
  INSERT (
    vr_program_id, address_line_1, address_city, address_zipcode, address_state,
    first_name, last_name, ncoa_status, last_updated, last_attempted_update
  )
  VALUES (
    source.vr_program_id, source.address_line_1, source.address_city, source.address_zipcode,
    source.address_state, source.first_name, source.last_name, source.ncoa_status,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  )
"""
client.query(query).result()

# Clean up the temporary table
client.delete_table(temp_table_id, not_found_ok=True)
