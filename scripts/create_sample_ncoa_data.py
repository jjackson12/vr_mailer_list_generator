import pandas as pd

# Define the path to the CSV file
input_file_path = "../job_request_input_id_6ee13e57-6639-4eb8-8e14-6c1295ba9e03.csv"

# Read the CSV file into a DataFrame
data = pd.read_csv(input_file_path)

# Display the first few rows of the DataFrame
print(data.head())

sample_output_data = [
    {
        "record_id": row.individual_id,
        "first_name": row.individual_first_name,
        "last_name": row.individual_last_name,
        "street_number": (
            row.address_line_1.split(" ")[0]
            if pd.notna(row.address_line_1) and len(row.address_line_1.split(" ")) > 0
            else ""
        ),
        "street_name": (
            " ".join(row.address_line_1.split(" ")[1:])
            if pd.notna(row.address_line_1) and len(row.address_line_1.split(" ")) > 1
            else ""
        ),
        "street_suffix": "",
        "city_name": row.address_city_name,
        "postal_code": row.address_postal_code,
        "address_status": "TODO",
        "vacant": "TODO",
        "move_applied": "TODO",
        "move_type": "TODO",
        "move_date": "TODO",
        "move_distance": "TODO",
        "match_flag": "TODO",
        "nxi": "TODO",
        "residential_delivery_indicator": "TODO",
        "record_type": "TODO",
        "address_line_1": row.address_line_1,
        "address_id": "TODO",
        "household_id": "TODO",
        "individual_id": "TODO",
    }
    for row in data.iterrows()
]

sample_output_df = pd.DataFrame(sample_output_data)
print(sample_output_df.head())
sample_output_df.to_csv("sample_ncoa_output.csv", index=False)
