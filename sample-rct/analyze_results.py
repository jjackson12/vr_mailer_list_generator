import pandas as pd
import os
import numpy as np
from scipy.stats import norm

EXPERIMENT_DIR = "/Users/jacobjackson/Dev/2025_projects/vr_mail_list_generator/sample-rct/sample_results_f5e254ba-1c74-4f1f-bdcd-4a3057f630f0"

control_path = os.path.join(EXPERIMENT_DIR, "generated_control.csv")
treatment_path = os.path.join(EXPERIMENT_DIR, "generated_treatment.csv")

control_df = pd.read_csv(control_path)
treatment_df = pd.read_csv(treatment_path)


def ab_test(x_t, n_t, x_c, n_c, alpha=0.05):
    # rates
    p_t, p_c = x_t / n_t, x_c / n_c
    diff = p_t - p_c

    # 95% CI for the lift (unpooled SE)
    se_unpooled = np.sqrt(p_t * (1 - p_t) / n_t + p_c * (1 - p_c) / n_c)
    zcrit = norm.ppf(1 - alpha / 2)
    ci = (diff - zcrit * se_unpooled, diff + zcrit * se_unpooled)

    # One-sided z-test (H1: p_t > p_c) using pooled SE
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

res = ab_test(x_t=x_t, n_t=n_t, x_c=x_c, n_c=n_c, alpha=0.05)
print("\n", res)
