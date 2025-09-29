import csv
import json
from google.cloud import storage

from streamlit.logger import get_logger

logger = get_logger(__name__)


def analyze_list_from_gcs(list_name, client):
    """
    Reads control/treatment CSVs and lift JSON from a GCS bucket for a given list_name.
    Returns a dict with the analysis results.
    """
    bucket_name = "vr_mail_lists"
    bucket = client.bucket(bucket_name)
    subfolder = f"list/{list_name}/sample_results"

    control_blob = bucket.blob(f"{subfolder}/generated_control.csv")
    treatment_blob = bucket.blob(f"{subfolder}/generated_treatment.csv")
    lift_blob = bucket.blob(f"{subfolder}/treatment_lift.json")

    def calc_behavior_percent(blob):
        if not blob.exists():
            return None
        content = blob.download_as_text()
        reader = csv.DictReader(content.splitlines())
        rows = list(reader)
        if not rows or "behavior" not in rows[0]:
            return None
        total = len(rows)
        behavior_1 = sum(1 for row in rows if row["behavior"] == "1")
        return (behavior_1 / total) if total > 0 else None

    control_pct = calc_behavior_percent(control_blob)
    treatment_pct = calc_behavior_percent(treatment_blob)

    lift = None
    base_behavior_rate = None
    if lift_blob.exists():
        try:
            data = json.loads(lift_blob.download_as_text())
            lift = data.get("treatment_lift")
            base_behavior_rate = data.get("base_behavior_rate")
        except Exception:
            lift = None
            base_behavior_rate = None

    result = {
        "list_name": list_name,
        "control_pct": control_pct,
        "treatment_pct": treatment_pct,
        "difference": (
            (treatment_pct - control_pct)
            if control_pct is not None and treatment_pct is not None
            else None
        ),
        "base_behavior_rate": base_behavior_rate,
        "treatment_lift": lift,
    }
    return result
