import pandas as pd
from google.cloud import storage
import io
import random
import os
import json
import logging
from streamlit.logger import get_logger

logger = get_logger(__name__)


class RCTResultGenerator:
    def __init__(
        self,
        list_name: str,
        base_behavior_rate: float,
        bucket_client: storage.Client,
        lift_range: tuple = (0, 0.05),
    ):
        self.list_name = list_name
        self.experiment_id = list_name
        self.base_behavior_rate = base_behavior_rate
        self.lift_range = lift_range

        self.client = bucket_client
        self.bucket_name = "vr_mail_lists"
        self.blob_prefix = f"lists/{self.list_name}/"
        self.control_file = f"{self.blob_prefix}control_group.csv"
        self.treatment_file = f"{self.blob_prefix}treatment_group.csv"
        self.bucket = self.client.bucket(self.bucket_name)

        self.control_df = self._download_csv(self.control_file)
        self.treatment_df = self._download_csv(self.treatment_file)

    def _download_csv(self, blob_path: str) -> pd.DataFrame:
        blob = self.bucket.blob(blob_path)
        csv_bytes = blob.download_as_bytes()
        return pd.read_csv(io.BytesIO(csv_bytes))

    def _upload_csv(self, df: pd.DataFrame, blob_path: str):
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    def _upload_json(self, data: dict, blob_path: str):
        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(data), content_type="application/json")

    def set_experiment_effects(self, set_lift: float = None) -> float:
        if set_lift is not None:
            treatment_lift = set_lift
        else:
            treatment_lift = random.uniform(*self.lift_range)
        json_data = {
            "experiment_id": self.experiment_id,
            "treatment_lift": treatment_lift,
            "base_behavior_rate": self.base_behavior_rate,
        }
        json_blob_path = f"lists/{self.list_name}/sample_results/treatment_lift.json"
        self._upload_json(json_data, json_blob_path)
        return treatment_lift

    def generate_outcomes(
        self, df: pd.DataFrame, treatment: bool, treatment_lift: float = None
    ) -> pd.DataFrame:
        df = df.copy()
        if treatment:
            behavior_rate = self.base_behavior_rate + treatment_lift
        else:
            behavior_rate = self.base_behavior_rate
        block_size = 100
        num_rows = len(df)
        behaviors = []
        for i in range(0, num_rows, block_size):
            block_end = min(i + block_size, num_rows)
            block = df.iloc[i:block_end]
            num_positive = int(round(behavior_rate * len(block)))
            block_behaviors = [1] * num_positive + [0] * (len(block) - num_positive)
            random.shuffle(block_behaviors)
            behaviors.extend(block_behaviors)
        df["behavior"] = behaviors
        return df

    def run(self, set_lift: float = None):
        treatment_lift = self.set_experiment_effects(set_lift)
        generated_control = self.generate_outcomes(self.control_df, treatment=False)
        generated_treatment = self.generate_outcomes(
            self.treatment_df, treatment=True, treatment_lift=treatment_lift
        )
        control_blob_path = (
            f"lists/{self.list_name}/sample_results/generated_control.csv"
        )
        treatment_blob_path = (
            f"lists/{self.list_name}/sample_results/generated_treatment.csv"
        )
        self._upload_csv(generated_control, control_blob_path)
        self._upload_csv(generated_treatment, treatment_blob_path)
        logger.info(
            f"Generated control group results uploaded to gs://{self.bucket_name}/{control_blob_path}"
        )
        logger.info(
            f"Generated treatment group results uploaded to gs://{self.bucket_name}/{treatment_blob_path}"
        )
