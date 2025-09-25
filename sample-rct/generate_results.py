import uuid
import pandas as pd
from google.cloud import storage
import io
import random
import os

# Generate a unique experiment ID
EXPERIMENT_ID = str(uuid.uuid4())

# Initialize Google Cloud Storage client
client = storage.Client.from_service_account_json(
    "/Users/jacobjackson/Dev/2025_projects/vr_mail_list_generator/vr-mail-generator-56bee8a8278b.json"
)

LIST_NAME = "test_dems_unaffiliateds"
BASE_BEHAVIOR_RATE = 0.3
LIFT_RANGE = (-0.02, 0.07)

bucket_name = "vr_mail_lists"
blob_prefix = f"lists/{LIST_NAME}/"
control_file = f"{blob_prefix}control_group.csv"
treatment_file = f"{blob_prefix}treatment_group.csv"

bucket = client.bucket(bucket_name)

control_blob = bucket.blob(control_file)
control_csv_bytes = control_blob.download_as_bytes()
control_df = pd.read_csv(io.BytesIO(control_csv_bytes))

treatment_blob = bucket.blob(treatment_file)
treatment_csv_bytes = treatment_blob.download_as_bytes()
treatment_df = pd.read_csv(io.BytesIO(treatment_csv_bytes))


def set_experiment_effects():
    treatment_lift = random.uniform(*LIFT_RANGE)
    folder_name = f"sample-rct/sample_results_{EXPERIMENT_ID}"
    os.makedirs(folder_name, exist_ok=True)
    json_path = os.path.join(folder_name, "treatment_lift.json")
    with open(json_path, "w") as f:
        f.write(
            f'{{"experiment_id": "{EXPERIMENT_ID}", "treatment_lift": {treatment_lift}, "base_behavior_rate": {BASE_BEHAVIOR_RATE}}}'
        )
    return treatment_lift


def generate_outcomes(
    df: pd.DataFrame, treatment: bool, treatment_lift: float = None
) -> pd.DataFrame:
    df = df.copy()
    if treatment:
        behavior_rate = BASE_BEHAVIOR_RATE + treatment_lift
    else:
        behavior_rate = BASE_BEHAVIOR_RATE

    df["behavior"] = df.apply(
        lambda row: 1 if random.random() < behavior_rate else 0, axis=1
    )
    return df


treatment_lift = set_experiment_effects()

generated_control = generate_outcomes(control_df, treatment=False)
generated_treatment = generate_outcomes(
    treatment_df, treatment=True, treatment_lift=treatment_lift
)
generated_control_path = os.path.join(
    f"sample-rct/sample_results_{EXPERIMENT_ID}", "generated_control.csv"
)
generated_treatment_path = os.path.join(
    f"sample-rct/sample_results_{EXPERIMENT_ID}", "generated_treatment.csv"
)

generated_control.to_csv(generated_control_path, index=False)
generated_treatment.to_csv(generated_treatment_path, index=False)
print(f"Generated control group results saved to {generated_control_path}")
