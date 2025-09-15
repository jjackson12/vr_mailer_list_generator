# app.py
from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple

import pandas as pd
import numpy as np
import streamlit as st
from config import BUCKETS_SERVICE_ACCOUNT_KEY, BUCKET_NAME

import plotly.express as px
from vr_list_generator import VRMailListGenerator

# GCS
try:
    from google.cloud import storage

except Exception as _e:
    storage = None  # We'll show a friendly error if missing
import zipfile
from io import BytesIO
from pytz import timezone as pytz_timezone

# TODO: Move this statistical analysis implementation to vr_list_generator.py
from statsmodels.stats.power import NormalIndPower
from statsmodels.stats.proportion import proportion_effectsize

# =============== Config ===============
# TODO: Could make the RCT part optional
st.set_page_config(page_title="Mailer RCT list generator", layout="wide")
generator = VRMailListGenerator()


# =============== Helper functions ===============


def parse_bucket_spec(spec: str) -> str:
    """Split a 'bucket/prefix' or 'gs://bucket/prefix' into (bucket, 'prefix/').
    Extract the last directory name if the prefix ends with '/*.csv'."""
    s = spec.replace("gs://", "").strip().lstrip("/")
    parts = s.split("/")
    name = parts[2]
    return name


def get_gcs_client() -> storage.Client:  # type: ignore
    """Create a GCS client from a keyfile path in BUCKETS_SERVICE_ACCOUNT_KEY env var."""
    key_path = BUCKETS_SERVICE_ACCOUNT_KEY
    if not key_path:
        st.error(
            "Environment variable BUCKETS_SERVICE_ACCOUNT_KEY is not set. "
            "Set it to the JSON key filepath for your service account."
        )
        st.stop()
    if not os.path.exists(key_path):
        st.error(f"Service account key file not found at: {key_path}")
        st.stop()
    if storage is None:
        st.error(
            "google-cloud-storage is not installed. Run `pip install google-cloud-storage`."
        )
        st.stop()
    return storage.Client.from_service_account_json(key_path)


@st.cache_data(show_spinner=False, ttl=120)
def load_past_lists_gcs(spec: str) -> pd.DataFrame:
    """List objects in gs://<bucket>/<prefix> and return list_name + created_at."""
    client = get_gcs_client()
    rows = []
    lists_data = {}
    for blob in client.list_blobs(BUCKET_NAME):
        # Skip "directory placeholders"
        if blob.name.endswith("/"):
            continue
        # list_name is the filename stem
        list_name = blob.name.strip().split("/")[-2]
        lists_data[list_name] = min(
            lists_data.get(list_name, blob.time_created or blob.updated),
            blob.time_created,
            blob.updated,
        )
    ny_tz = pytz_timezone("America/New_York")
    list_df = pd.DataFrame(
        [
            {
                "list_name": l,
                "created_at": c.astimezone(ny_tz).strftime("%Y-%m-%d %H:%M %Z"),
            }
            for l, c in lists_data.items()
        ]
    ).sort_values("created_at", ascending=False)

    return list_df


def parse_csv_list(value: str) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def compute_households(df: pd.DataFrame) -> int:
    addr_cols = ["mail_addr1", "mail_addr2", "mail_city", "mail_state", "mail_zipcode"]
    for c in addr_cols:
        if c not in df.columns:
            df[c] = ""
    return df[addr_cols].fillna("").drop_duplicates().shape[0]


def ensure_list_name_safe(name: str) -> str:
    if not name or name == "":
        return None
    safe = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in name.strip()
    )
    return safe[:120] if safe else ""


def filter_voters(params: Dict[str, Any]) -> pd.DataFrame:
    list_generator = generator
    return list_generator.filter_voters(params)


# =============== Auth gate (name + email) ===============
if "user_info" not in st.session_state:
    st.session_state.user_info = {}

if not st.session_state.user_info.get("name") or not st.session_state.user_info.get(
    "email"
):
    st.title("Welcome!")
    st.write("Please enter your details to continue.")
    with st.form("user_form", clear_on_submit=False):
        name = st.text_input("Your name", placeholder="Jane Doe")
        email = st.text_input("Email", placeholder="jane@example.org")
        agreed = st.checkbox(
            "I agree to store my name and email for this session.", value=True
        )
        submitted = st.form_submit_button("Continue")
    if submitted:
        if not name or not email:
            st.error("Name and email are required.")
            st.stop()
        if not agreed:
            st.error("Please check the box to continue.")
            st.stop()
        st.session_state.user_info = {"name": name.strip(), "email": email.strip()}
        st.rerun()
    st.stop()

# =============== Main App =================
st.title("Mailer RCT List Generator")
st.caption(
    f"Signed in as **{st.session_state.user_info['name']}** • {st.session_state.user_info['email']}"
)


def download_and_zip_files(list_name: str) -> BytesIO:
    """Download files from GCS for a given list_name, zip them, and return as BytesIO."""
    client = get_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    prefix = f"lists/{list_name}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        raise FileNotFoundError(f"No files found under gs://{BUCKET_NAME}/{prefix}")

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zipf:
        for blob in blobs:
            local_file = blob.name.replace("/", "__")
            blob_data = blob.download_as_bytes()
            zipf.writestr(local_file, blob_data)
    zip_buffer.seek(0)
    return zip_buffer


with st.expander("Saved Lists", expanded=True):
    try:
        past_df = load_past_lists_gcs(BUCKET_NAME + "/lists")
        if past_df.empty:
            st.info(f"No lists found yet in gs://{BUCKET_NAME}/lists/.")
        else:
            # Pagination setup
            items_per_page = 10
            total_items = len(past_df)
            total_pages = (total_items + items_per_page - 1) // items_per_page
            current_page = st.session_state.get("pagination_input", 1)

            # Calculate start and end indices for the current page
            start_idx = (current_page - 1) * items_per_page
            end_idx = start_idx + items_per_page
            page_df = past_df.iloc[start_idx:end_idx]

            for _, row in page_df.iterrows():
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"**{row['list_name']}**")
                    st.caption(
                        f"Created at: {row['created_at']}"
                    )  # Use caption for smaller text
                with col2:
                    if st.button(f"Prepare for download", key=row["list_name"]):
                        try:
                            zip_file = download_and_zip_files(row["list_name"])
                            st.download_button(
                                label="Download ZIP",
                                data=zip_file,
                                file_name=f"{row['list_name']}.zip",
                                mime="application/zip",
                                type="primary",
                            )
                        except Exception as e:
                            st.error(f"Error downloading files: {e}")

            # Display pagination info at the bottom
            st.write(f"Page {current_page} of {total_pages}")
            current_page = st.number_input(
                "Page",
                min_value=1,
                max_value=total_pages,
                value=current_page,
                step=1,
                key="pagination_input",
            )
    except Exception as e:
        st.error(f"Error loading past lists from GCS: {e}")
        st.button(
            "Refresh list", on_click=lambda: load_past_lists_gcs.clear()
        )  # clears cache
    # Add a "Refresh Lists" button at the bottom
    if st.button("Refresh Lists"):
        load_past_lists_gcs.clear()
        st.rerun()

# with st.expander("Past lists (from GCS)", expanded=True):
#     try:
#         past_df = load_past_lists_gcs(GCS_BUCKET_SPEC)
#         if past_df.empty:
#             st.info("No lists found yet in gs://vr_mail_lists/lists/.")
#         else:
#             st.dataframe(
#                 past_df[["list_name", "created_at"]],
#                 use_container_width=True,
#                 hide_index=True,
#             )
#         st.button(
#             "Refresh list", on_click=lambda: load_past_lists_gcs.clear()
#         )  # clears cache
#     except Exception as e:
#         st.error(f"Error loading past lists from GCS: {e}")

st.markdown("---")

# ---------- Search Criteria ----------
st.subheader("Search criteria")
st.caption(
    "NOTE: Currently, this is referencing only the voters in Yadkin County, North Carolina. Note that this is REAL and PUBLIC data, collected from the North Carolina State Board of Elections, available here: https://dl.ncsbe.gov/"
)

c1, c2 = st.columns([3, 2], gap="large")
with c1:
    counties = [
        "Alamance",
        "Alexander",
        "Alleghany",
        "Anson",
        "Ashe",
        "Avery",
        "Beaufort",
        "Bertie",
        "Bladen",
        "Brunswick",
        "Buncombe",
        "Burke",
        "Cabarrus",
        "Caldwell",
        "Camden",
        "Carteret",
        "Caswell",
        "Catawba",
        "Chatham",
        "Cherokee",
        "Chowan",
        "Clay",
        "Cleveland",
        "Columbus",
        "Craven",
        "Cumberland",
        "Currituck",
        "Dare",
        "Davidson",
        "Davie",
        "Duplin",
        "Durham",
        "Edgecombe",
        "Forsyth",
        "Franklin",
        "Gaston",
        "Gates",
        "Graham",
        "Granville",
        "Greene",
        "Guilford",
        "Halifax",
        "Harnett",
        "Haywood",
        "Henderson",
        "Hertford",
        "Hoke",
        "Hyde",
        "Iredell",
        "Jackson",
        "Johnston",
        "Jones",
        "Lee",
        "Lenoir",
        "Lincoln",
        "McDowell",
        "Macon",
        "Madison",
        "Martin",
        "Mecklenburg",
        "Mitchell",
        "Montgomery",
        "Moore",
        "Nash",
        "New Hanover",
        "Northampton",
        "Onslow",
        "Orange",
        "Pamlico",
        "Pasquotank",
        "Pender",
        "Perquimans",
        "Person",
        "Pitt",
        "Polk",
        "Randolph",
        "Richmond",
        "Robeson",
        "Rockingham",
        "Rowan",
        "Rutherford",
        "Sampson",
        "Scotland",
        "Stanly",
        "Stokes",
        "Surry",
        "Swain",
        "Transylvania",
        "Tyrrell",
        "Union",
        "Vance",
        "Wake",
        "Warren",
        "Washington",
        "Watauga",
        "Wayne",
        "Wilkes",
        "Wilson",
        "Yadkin",
        "Yancey",
    ]
    selected_counties = st.multiselect(
        "County",
        options=counties,
        help="Select one or more counties from the list.",
    )
    party = st.multiselect(
        "Party",
        options=["DEM", "REP", "UNA", "LIB", "GRE", "OTH"],
        help="Select one or more party codes.",
    )
    race = st.multiselect(
        "Race",
        options=["White", "Black", "Asian", "Native American", "Other", "Unknown"],
    )
    prepare_race_subgroup = st.checkbox(
        "Prepare for subgroup analysis", key="race_subgroup"
    )
    ethnicity = st.multiselect(
        "Ethnicity",
        options=["Hispanic/Latino", "Non-Hispanic", "Unknown"],
    )
    prepare_ethnicity_subgroup = st.checkbox(
        "Prepare for subgroup analysis", key="ethnicity_subgroup"
    )
    gender = st.multiselect(
        "Gender",
        options=["Male", "Female", "Undesignated"],
    )
    prepare_gender_subgroup = st.checkbox(
        "Prepare for subgroup analysis", key="gender_subgroup"
    )
with c2:
    age_min, age_max = st.slider(
        "Age range",
        min_value=18,
        max_value=100,
        value=(18, 100),
        step=1,
        help="Inclusive bounds",
    )
    prepare_age_subgroup = st.checkbox(
        "Prepare for subgroup analysis", key="age_subgroup"
    )
    st.write("**Districts**")
    state_house = st.multiselect("State House", options=list(range(1, 78)))
    state_senate = st.multiselect("State Senate", options=list(range(1, 37)))
    congressional = st.multiselect("Congressional", options=list(range(1, 11)))

# Human-readable params (kept as-is for downstream request payloads)
params: Dict[str, Any] = {
    "County": selected_counties,
    "Party": party,
    "Race": race,
    "Ethnicity": ethnicity,
    "Gender": gender,
    "Age": [age_min, age_max],
    "state_house": state_house,
    "state_senate": state_senate,
    "congressional": congressional,
}

# ---- Mapping to codes BEFORE calling filter_voters ----

GENDER_MAP = {"Male": "M", "Female": "F", "Undesignated": "U"}
RACE_MAP = {
    "White": "W",
    "Black": "B",
    "Asian": "A",
    "Native American": "I",
    "Other": "O",
    "Unknown": "U",
    "Two or More Races": "M",
    "Native Hawaiian or Pacific Islander": "P",
}
ETHNICITY_MAP = {"Hispanic/Latino": "HL", "Non-Hispanic": "NL", "Unknown": "UN"}


# Example county code map (optional). If your data expects FIPS/short codes, map here.
# Leave as provided names if not needed.
def map_county_names_to_codes(counties: List[str]) -> List[str]:
    # Placeholder: return uppercase names, or implement a real mapping (e.g., {"Denver": "DEN"})
    return [c.strip().upper() for c in counties]


def map_param_codes(in_params):
    params_codes: Dict[str, Any] = {
        "County": map_county_names_to_codes(in_params["County"]),
        "Party": in_params["Party"],
        "Race": [RACE_MAP.get(x, x) for x in in_params["Race"]],
        "Ethnicity": [ETHNICITY_MAP.get(x, x) for x in in_params["Ethnicity"]],
        "Gender": [GENDER_MAP.get(x, x) for x in in_params["Gender"]],
        "Age": in_params["Age"],
        "StateHouseDistrict": in_params["state_house"],
        "StateSenateDistrict": in_params["state_senate"],
        "CongressionalDistrict": in_params["congressional"],
    }
    return params_codes


st.divider()
# Action buttons
generate_clicked = st.button(
    "Run List",
    # type="primary",
    help="Click to generate counts based on the selected criteria.",
    key="blue_button",
)

# Keep last results in session
if "last_df" not in st.session_state:
    st.session_state.last_df = None
if "last_params_codes" not in st.session_state:
    st.session_state.last_params_codes = None

# ---------- Generate counts ----------
if generate_clicked:
    try:
        df = filter_voters(map_param_codes(params))  # NOTE: pass mapped codes
        st.session_state.last_df = df
        st.session_state.last_params_codes = map_param_codes(params)
    except Exception as e:
        st.error(f"Error generating counts: {e}")
        st.stop()

if st.session_state.last_df is not None:
    df = st.session_state.last_df
    people = int(len(df))
    households = compute_households(df)

    # st.success(f"**# people:** {people}   •   **# households (with valid addresses):** {households:,}")
    c1, c2, c3 = st.columns(3)

    with c1:
        st.success(f"**# people:** {people}")

    with c2:
        st.success(f"**# households (with valid addresses):** {households:,}")

    with c3:
        try:
            invalid_targets = len(generator.get_invalid_targets(df))
            st.success(
                f"**# people with missing or invalid addresses:** {invalid_targets}"
            )
        except Exception as e:
            st.error(f"Error calculating valid people: {e}")

    st.divider()
    st.subheader("List Details")
    # st.caption(
    #     "This will submit a request to generate the mailing list, which will then appear in the list at the top of the page for download"
    # )
    categorical_columns = ["Race", "Ethnicity", "Gender", "Party"]
    with st.expander("Show list breakdowns by subgroups", expanded=False):
        pie_charts = []
        for col in categorical_columns:
            renamed_col = generator.RENAME_MAP[col]
            if renamed_col in df.columns and not df[renamed_col].isna().all():
                col_counts = df[renamed_col].value_counts()
                color_map = {
                    "DEM": "darkblue",
                    "REP": "red",
                    "UNA": "gray",
                    "LIB": "gold",
                    "GRE": "green",
                    "OTH": "purple",
                }
                fig = px.pie(
                    col_counts,
                    values=col_counts.values,
                    names=col_counts.index,
                    hole=0.4,
                    title=f"Distribution of {col}",
                    color=col_counts.index,
                    color_discrete_map=color_map if col == "Party" else None,
                )
                pie_charts.append(fig)
            else:
                st.warning(f"{col} column not found in results.")

        # Display pie charts in a 2x2 grid
        if pie_charts:
            rows = (len(pie_charts) + 1) // 2
            for i in range(rows):
                cols = st.columns(2)
                for j in range(2):
                    idx = i * 2 + j
                    if idx < len(pie_charts):
                        with cols[j]:
                            st.plotly_chart(pie_charts[idx], use_container_width=True)

        renamed_age = generator.RENAME_MAP["Age"]
        # Histogram for age
        if renamed_age in df.columns and not df[renamed_age].isna().all():
            df["age_bucket"] = pd.cut(
                df[renamed_age],
                bins=[18, 35, 50, 65, 100],
                right=False,
                labels=["18-34", "35-49", "50-64", "65+"],
            )
            age_counts = df["age_bucket"].value_counts().sort_index()
            fig = px.bar(
                age_counts,
                x=age_counts.index.astype(str),
                y=age_counts.values,
                labels={"x": "Age Range", "y": "Count"},
                title="Age Distribution",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Age column not found in results.")

    with st.expander("Preview rows", expanded=False):
        st.dataframe(df.head(200), use_container_width=True)
    st.divider()

st.subheader("RCT Control Grouping and Mailing List Generator")
st.caption(
    "This will submit a request to generate a randomized control group, then a mailing list of the treatment group aggregated to the household level. These lists will then appear in the list at the top of the page for download"
)
if st.session_state.last_df is None:
    st.info("Generate a list first to enable RCT list generating.")
else:
    with st.container():
        # c1, c2, c3 = st.columns([3, 2])
        # with c1:
        control_proportion_input = st.number_input(
            "Control group proportion (%)",
            min_value=10.0,
            max_value=50.0,
            value=50.0,
            step=1.0,
            help="Specify the percentage of the sample in the control group (e.g., 50 for 50%, or 10% to send mail to 9 out of 10 people on this list and reserve the last 10% as a control group).",
        )
        # with c2:

        # List name for submission
        list_name_input = st.text_input(
            "List name (required to submit)",
            placeholder="e.g., denver-young-dems-2025-09-10",
        )
        # with c3:
        submit_clicked = st.button(
            "Generate RCT groups and Mailing List", type="primary"
        )
    # ---------- Submit list request (no local writes) ----------
    if submit_clicked:
        safe_name = ensure_list_name_safe(list_name_input)
        if not safe_name:
            st.error(
                "Please provide a valid list name (letters, numbers, hyphens/underscores)."
            )
            st.stop()

        # Ensure we have data to submit
        if st.session_state.last_df is None:
            try:
                st.session_state.last_df = filter_voters(map_param_codes(params))
                st.session_state.last_params_codes = map_param_codes(params)
            except Exception as e:
                st.error(f"Could not generate data for submission: {e}")
                st.stop()

        subgroup_variables = [
            var
            for var, prepare in [
                ("Race", prepare_race_subgroup),
                ("Ethnicity", prepare_ethnicity_subgroup),
                ("Gender", prepare_gender_subgroup),
                ("Age", prepare_age_subgroup),
            ]
            if prepare
        ]
        # try:
        generator.generate_rct_mailing_list(
            list_df=st.session_state.last_df,
            requestor_email=st.session_state.user_info["email"],
            requestor_name=st.session_state.user_info["name"],
            request_name=safe_name,
            params=map_param_codes(params),
            control_prop=control_proportion_input,
        )
        st.success(
            f"List request **{safe_name}** submitted. You’ll see it appear in the 'Saved Lists' table above once it is ready (a few seconds typically)."
        )
        # Soft refresh of the cached listing
        load_past_lists_gcs.clear()
        # except NameError:
        #     st.error(
        #         "`generate_rct_mailing_list` is not defined/imported in this app. "
        #         "Import it or ensure it’s on PYTHONPATH."
        #     )
        # except Exception as e:
        #     st.error(f"Failed to submit list request: {e}")
# Power analysis section
st.markdown("---")
st.subheader("Power Analysis")

if st.session_state.last_df is None:
    st.info("Generate a list first to enable power analysis for subgroups.")
else:
    # Input fields for power analysis
    baseline_rate = st.number_input(
        "Expected baseline rate (e.g., % who will register to vote without receiving mail)",
        min_value=0.0,
        max_value=100.0,
        value=10.0,
        step=0.1,
        format="%.1f",
        help="Enter the baseline rate as a percentage (e.g., 10 for 10%).",
    )

    control_proportion = st.number_input(
        "% of sample size in control group",
        min_value=10.0,
        max_value=90.0,
        value=50.0,
        step=0.1,
        format="%.1f",
        help="Enter the percentage of the sample in the control group (e.g., 50 for 50%).",
    )

    min_lift = st.number_input(
        "Minimum detectable percentage points (pp) 'lift' (e.g., 1%). Higher lift = smaller required sample size",
        min_value=0.1,
        max_value=100.0,
        value=5.0,
        step=0.1,
        format="%.1f",
        help="Enter the minimum detectable lift as a percentage (e.g., 1 for 1%).",
    )

    if st.button(
        "Generate power analysis for total population + subgroups of interest"
    ):
        try:
            # Convert percentages to proportions
            p_baseline_control = baseline_rate / 100
            min_abs_lift = min_lift / 100
            p_treat = p_baseline_control + min_abs_lift
            control_proportion = control_proportion / 100
            # TODO: This needs to reference the real breakdowns for the subgroups, since it's randomized
            ratio = (1 - control_proportion) / control_proportion  # n_treat / n_control

            # Calculate effect size and required sample size
            h = proportion_effectsize(p_baseline_control, p_treat)  # Cohen's h
            analysis = NormalIndPower()
            n_control = analysis.solve_power(
                effect_size=h,
                alpha=0.05,
                power=0.80,
                ratio=ratio,
                alternative="two-sided",
            )
            n_treat = ratio * n_control
            N_total = n_control + n_treat

            # Display required sample size
            st.success(
                f"**Approximate required sample size:** {int(round(N_total, -2)):,} "
                f"(Control: {int(round(n_control, -2)):,}, Treatment: {int(round(n_treat, -2)):,})"
            )

            # Display current list sample size
            current_sample_size = len(df)
            if current_sample_size >= N_total:
                st.success(
                    f"**Current list sample size:** {current_sample_size:,} (Meets or exceeds required sample size)"
                )
            else:
                st.error(
                    f"**Current list sample size:** {current_sample_size:,} (Below required sample size)"
                )

            # Subgroup analysis
            # TODO: I might need to call generate_counts here if last_df is empty?
            if st.session_state.last_df is not None:
                df = st.session_state.last_df
                # TODO: This should reference input variables for subgroup analysis
                subgroup_variables = [
                    var
                    for var, prepare in [
                        ("Race", prepare_race_subgroup),
                        ("Ethnicity", prepare_ethnicity_subgroup),
                        ("Gender", prepare_gender_subgroup),
                        ("Age", prepare_age_subgroup),
                    ]
                    if prepare
                ]
                st.caption(
                    "Below, subgroups highlighted in green meet the required sample size to likely have a statistically significant result given the input experiment specifications/constraints."
                )
                st.caption(
                    "If no subgroups appear, you need to check the box(es) above for 'Prepare for subgroup analysis'."
                )
                # TODO: Age into buckets
                for subgroup in subgroup_variables:
                    renamed_var = generator.RENAME_MAP[subgroup]
                    if renamed_var in df.columns:
                        if subgroup == "Age":
                            df["age_bucket"] = pd.cut(
                                df[renamed_var],
                                bins=[18, 35, 50, 65, 100],
                                right=False,
                                labels=["18-34", "35-49", "50-64", "65+"],
                            )
                            renamed_var = "age_bucket"
                        st.subheader(f"Subgroup: {subgroup}")
                        subgroup_counts = df[renamed_var].value_counts().reset_index()
                        subgroup_counts.columns = [subgroup, "Sample Size"]
                        subgroup_counts = subgroup_counts.sort_values(
                            "Sample Size", ascending=False
                        )

                        # Highlight rows based on sample size
                        def highlight_row(row):
                            if row["Sample Size"] >= N_total:
                                return ["background-color: lightgreen"] * len(row)
                            else:
                                return ["background-color: lightcoral"] * len(row)

                        st.dataframe(
                            subgroup_counts.style.apply(
                                highlight_row, axis=1, subset=["Sample Size"]
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )
        except Exception as e:
            st.error(f"Error performing power analysis: {e}")
# =============== Footer ===============
st.markdown("---")
# st.caption(
#     "Notes: Past lists are read from GCS at gs://vr_mail_lists/lists/. "
#     "Before querying, UI selections are mapped to codes (e.g., Male→M, Black→B). "
#     "Replace the demo `filter_voters(params_codes)` with your real implementation."
# )
