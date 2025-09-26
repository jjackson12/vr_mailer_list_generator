import os
import csv
import json

base_dir = os.path.dirname(os.path.abspath(__file__))

for subfolder in os.listdir(base_dir):
    subfolder_path = os.path.join(base_dir, subfolder)
    if os.path.isdir(subfolder_path) and subfolder != os.path.basename(__file__):
        control_csv = os.path.join(subfolder_path, "generated_control.csv")
        treatment_csv = os.path.join(subfolder_path, "generated_treatment.csv")
        lift_json = os.path.join(subfolder_path, "treatment_lift.json")

        def calc_behavior_percent(csv_path):
            if not os.path.exists(csv_path):
                return None
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if not rows or "behavior" not in rows[0]:
                    return None
                total = len(rows)
                behavior_1 = sum(1 for row in rows if row["behavior"] == "1")
                return (behavior_1 / total) if total > 0 else None

        control_pct = calc_behavior_percent(control_csv)
        treatment_pct = calc_behavior_percent(treatment_csv)

        lift = None
        base_behavior_rate = None
        if os.path.exists(lift_json):
            with open(lift_json) as f:
                try:
                    data = json.load(f)
                    lift = data.get("treatment_lift")
                    base_behavior_rate = data.get("base_behavior_rate")
                except Exception:
                    lift = None
                    base_behavior_rate = None

        print(f"\n\nSubfolder: {subfolder}")
        print(
            f"  Control % behavior=1: {control_pct if control_pct is not None else 'N/A'}"
        )
        print(
            f"  Treatment % behavior=1: {treatment_pct if treatment_pct is not None else 'N/A'}"
        )
        if control_pct is not None and treatment_pct is not None:
            diff = treatment_pct - control_pct
            print(f"  Difference (Treatment - Control): {diff:.2f}")
        else:
            print("  Difference (Treatment - Control): N/A")
        print(
            f"\nEXPERIMENT_GENERATION_SPECS\n      Base behavior rate: {base_behavior_rate if base_behavior_rate is not None else 'N/A'}"
        )
        print(f"      Treatment lift: {lift if lift is not None else 'N/A'}")
        print("-" * 40)
