import pandas as pd
import numpy as np
from scipy.stats import norm

from streamlit.logger import get_logger
from io import StringIO

logger = get_logger(__name__)


def analyze_results(bucket_client, list_name_input, alpha=0.05, subgroup_vars=None):
    experiment_dir = f"lists/{list_name_input}/sample_results"
    control_path = f"{experiment_dir}/generated_control.csv"
    treatment_path = f"{experiment_dir}/generated_treatment.csv"

    # Access the bucket and blobs
    bucket = bucket_client.bucket("vr_mail_lists")
    control_blob = bucket.blob(control_path)
    treatment_blob = bucket.blob(treatment_path)

    # Download CSV data as text
    control_csv = control_blob.download_as_text()
    treatment_csv = treatment_blob.download_as_text()

    # Read into DataFrames
    control_df = pd.read_csv(StringIO(control_csv))
    treatment_df = pd.read_csv(StringIO(treatment_csv))

    def ab_test(x_t, n_t, x_c, n_c, alpha=0.05):
        p_t, p_c = x_t / n_t, x_c / n_c
        diff = p_t - p_c
        se_unpooled = np.sqrt(p_t * (1 - p_t) / n_t + p_c * (1 - p_c) / n_c)
        zcrit = norm.ppf(1 - alpha / 2)
        ci = (diff - zcrit * se_unpooled, diff + zcrit * se_unpooled)
        p_pool = (x_t + x_c) / (n_t + n_c)
        se_pool = np.sqrt(p_pool * (1 - p_pool) * (1 / n_t + 1 / n_c))
        z = diff / se_pool
        p_one_sided = 1 - norm.cdf(z)
        p_two_sided = 2 * (1 - norm.cdf(abs(z)))
        return {
            "p_treat": p_t,
            "p_control": p_c,
            "lift": diff,
            "ci95_lift": ci,
            "z": z,
            "p_pos_one_sided": p_one_sided,
            "p_two_sided": p_two_sided,
        }

    if subgroup_vars:
        results = {}
        for var in subgroup_vars:
            results[var] = {}
            control_groups = control_df.groupby(var)
            treatment_groups = treatment_df.groupby(var)
            subgroups = set(control_df[var].unique()).union(
                set(treatment_df[var].unique())
            )
            for subgroup in subgroups:
                control_sub = (
                    control_groups.get_group(subgroup)
                    if subgroup in control_groups.groups
                    else pd.DataFrame(columns=control_df.columns)
                )
                treatment_sub = (
                    treatment_groups.get_group(subgroup)
                    if subgroup in treatment_groups.groups
                    else pd.DataFrame(columns=treatment_df.columns)
                )
                x_t = treatment_sub["behavior"].sum()
                n_t = len(treatment_sub)
                x_c = control_sub["behavior"].sum()
                n_c = len(control_sub)
                if n_t > 0 and n_c > 0:
                    results[var][subgroup] = ab_test(
                        x_t=x_t, n_t=n_t, x_c=x_c, n_c=n_c, alpha=alpha
                    )
                else:
                    results[var][subgroup] = None
        return results
    else:
        x_t = treatment_df["behavior"].sum()
        n_t = len(treatment_df)
        x_c = control_df["behavior"].sum()
        n_c = len(control_df)

        # TODO: Copy type from if subgroup_vars return statement, then adapt in the frontend
        res = ab_test(x_t=x_t, n_t=n_t, x_c=x_c, n_c=n_c, alpha=alpha)
    return res
