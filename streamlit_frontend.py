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

from vr_list_generator import VRMailListGenerator

# GCS
try:
    from google.cloud import storage

except Exception as _e:
    storage = None  # We'll show a friendly error if missing
import zipfile
from io import BytesIO
from pytz import timezone as pytz_timezone

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


def get_gcs_client() -> storage.Client:
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
st.title("Voter List Builder")
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


# List name for submission
list_name_input = st.text_input(
    "List name (required to submit)", placeholder="e.g., denver-young-dems-2025-09-10"
)

# Action buttons
b1, b2 = st.columns([1, 2], gap="large")
generate_clicked = b1.button("Generate counts", type="secondary")
submit_clicked = b2.button("Submit list request", type="primary")

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
            invalid_targets = len(generator.get_invalid_addresses(df))
            st.success(
                f"**# people with missing or invalid addresses:** {invalid_targets}"
            )
        except Exception as e:
            st.error(f"Error calculating valid people: {e}")

    # Optional stats dropdown
    stat_choice = st.selectbox(
        "Optional statistics", options=["None", "Median age"], index=0
    )
    if stat_choice == "Median age":
        if "age" in df.columns and not df["age"].isna().all():
            median_age = int(float(df["age"].median()))
            st.info(f"**Median age:** {median_age}")
        else:
            st.warning("Age column not found in results.")

    with st.expander("Preview rows", expanded=False):
        st.dataframe(df.head(200), use_container_width=True)

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
        stratification_vars=subgroup_variables,
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

# =============== Footer ===============
st.markdown("---")
# st.caption(
#     "Notes: Past lists are read from GCS at gs://vr_mail_lists/lists/. "
#     "Before querying, UI selections are mapped to codes (e.g., Male→M, Black→B). "
#     "Replace the demo `filter_voters(params_codes)` with your real implementation."
# )
