import pandas as pd
import numpy as np
from scipy.stats import norm


def analyze_results(bucket_client, list_name_input, alpha=0.05):
    experiment_dir = f"lists/{list_name_input}/sample_results"
    control_path = f"{experiment_dir}/generated_control.csv"
    treatment_path = f"{experiment_dir}/generated_treatment.csv"

    # Read CSVs from the "vr_mail_lists" bucket using the provided client
    control_df = pd.read_csv(
        bucket_client.download_as_bytes("vr_mail_lists", control_path)
    )
    treatment_df = pd.read_csv(
        bucket_client.download_as_bytes("vr_mail_lists", treatment_path)
    )

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

    x_t = treatment_df["behavior"].sum()
    n_t = len(treatment_df)
    x_c = control_df["behavior"].sum()
    n_c = len(control_df)

    res = ab_test(x_t=x_t, n_t=n_t, x_c=x_c, n_c=n_c, alpha=alpha)
    return res
