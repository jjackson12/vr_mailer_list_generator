# app.py
from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import pandas as pd
import numpy as np
import streamlit as st
from config import BUCKETS_SERVICE_ACCOUNT_KEY

# GCS
try:
    from google.cloud import storage
except Exception as _e:
    storage = None  # We'll show a friendly error if missing

# =============== Config ===============
# TODO: Could make the RCT part optional
st.set_page_config(page_title="Mailer RCT list generator", layout="wide")

# Bucket spec like "vr_mail_lists/lists" or "gs://vr_mail_lists/lists"
GCS_BUCKET_SPEC = "vr_mail_lists/lists"  # bucket "vr_mail_lists", prefix "lists/"

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
    for blob in client.list_blobs("vr_mail_lists"):
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
    list_df = pd.DataFrame(
        [
            {
                "list_name": l,
                "created_at": c.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
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


# ---- Replace this with your real data filter ----
def filter_voters(params: Dict[str, Any]) -> pd.DataFrame:
    """
    DEMO stub: returns a fake voter dataframe.
    Must return columns:
      - age (int)
      - mail_addr1, mail_addr2, mail_city, mail_state, mail_zipcode
    """
    rng = np.random.default_rng(42)
    n = rng.integers(500, 5000)

    ages = rng.integers(
        params["age_min"], max(params["age_max"], params["age_min"] + 1), size=n
    )

    streets = rng.choice([f"{num} Main St" for num in range(10, 999)], size=n)
    addr2s = rng.choice(["", "Apt 1", "Apt 2", "Unit B", ""], size=n)
    cities = rng.choice(
        ["Denver", "Boulder", "Aurora", "Fort Collins", "Pueblo"], size=n
    )
    states = rng.choice(["CO"], size=n)
    zips = rng.choice(["80202", "80301", "80012", "80521", "81003"], size=n)

    df = pd.DataFrame(
        {
            "age": ages,
            "mail_addr1": streets,
            "mail_addr2": addr2s,
            "mail_city": cities,
            "mail_state": states,
            "mail_zipcode": zips,
            # Echo back some params (likely codes after mapping)
            "county": rng.choice(params.get("county") or ["DEN"], size=n),
            "party": rng.choice(params.get("party") or ["D", "R", "U"], size=n),
            "race": rng.choice(params.get("race") or ["W", "B", "A", "O", "U"], size=n),
            "ethnicity": rng.choice(params.get("ethnicity") or ["H", "N", "U"], size=n),
            "gender": rng.choice(params.get("gender") or ["M", "F", "X", "U"], size=n),
            "state_house": rng.choice(
                params.get("state_house") or list(range(1, 66)), size=n
            ),
            "state_senate": rng.choice(
                params.get("state_senate") or list(range(1, 36)), size=n
            ),
            "congressional": rng.choice(
                params.get("congressional") or list(range(1, 9)), size=n
            ),
        }
    )
    return df


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

with st.expander("Past lists (from GCS)", expanded=True):
    try:
        past_df = load_past_lists_gcs(GCS_BUCKET_SPEC)
        if past_df.empty:
            st.info("No lists found yet in gs://vr_mail_lists/lists/.")
        else:
            st.dataframe(
                past_df[["list_name", "created_at"]],
                use_container_width=True,
                hide_index=True,
            )
        st.button(
            "Refresh list", on_click=lambda: load_past_lists_gcs.clear()
        )  # clears cache
    except Exception as e:
        st.error(f"Error loading past lists from GCS: {e}")

st.markdown("---")

# ---------- Search Criteria ----------
st.subheader("Search criteria")


def parse_csv_list(value: str) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


c1, c2 = st.columns([3, 2], gap="large")
with c1:
    county_csv = st.text_input(
        "County (comma-separated)",
        placeholder="e.g., Denver, Boulder, Arapahoe",
        help="Type one or more counties, separated by commas.",
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
    ethnicity = st.multiselect(
        "Ethnicity",
        options=["Hispanic/Latino", "Non-Hispanic", "Unknown"],
    )
    gender = st.multiselect(
        "Gender",
        options=["Male", "Female", "Nonbinary/Other", "Unknown"],
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
    st.write("**Districts**")
    state_house = st.multiselect("State House", options=list(range(1, 66)))
    state_senate = st.multiselect("State Senate", options=list(range(1, 36)))
    congressional = st.multiselect("Congressional", options=list(range(1, 9)))

# Human-readable params (kept as-is for downstream request payloads)
params: Dict[str, Any] = {
    "county": parse_csv_list(county_csv),
    "party": party,
    "race": race,
    "ethnicity": ethnicity,
    "gender": gender,
    "age_min": int(age_min),
    "age_max": int(age_max),
    "state_house": state_house,
    "state_senate": state_senate,
    "congressional": congressional,
}

# ---- Mapping to codes BEFORE calling filter_voters ----
# Adjust these to match your data dictionary.
PARTY_MAP = {"DEM": "D", "REP": "R", "UNA": "U", "LIB": "L", "GRE": "G", "OTH": "O"}
GENDER_MAP = {"Male": "M", "Female": "F", "Nonbinary/Other": "X", "Unknown": "U"}
RACE_MAP = {
    "White": "W",
    "Black": "B",
    "Asian": "A",
    "Native American": "N",
    "Other": "O",
    "Unknown": "U",
}
ETHNICITY_MAP = {"Hispanic/Latino": "H", "Non-Hispanic": "N", "Unknown": "U"}


# Example county code map (optional). If your data expects FIPS/short codes, map here.
# Leave as provided names if not needed.
def map_county_names_to_codes(counties: List[str]) -> List[str]:
    # Placeholder: return uppercase names, or implement a real mapping (e.g., {"Denver": "DEN"})
    return [c.strip().upper() for c in counties]


params_codes: Dict[str, Any] = {
    "county": map_county_names_to_codes(params["county"]),
    "party": [PARTY_MAP.get(x, x) for x in params["party"]],
    "race": [RACE_MAP.get(x, x) for x in params["race"]],
    "ethnicity": [ETHNICITY_MAP.get(x, x) for x in params["ethnicity"]],
    "gender": [GENDER_MAP.get(x, x) for x in params["gender"]],
    "age_min": params["age_min"],
    "age_max": params["age_max"],
    "state_house": params["state_house"],
    "state_senate": params["state_senate"],
    "congressional": params["congressional"],
}

# List name for submission
list_name_input = st.text_input(
    "List name (required to submit)", placeholder="e.g., denver-young-dems-2025-09-10"
)

# Action buttons
b1, b2 = st.columns([1, 1], gap="large")
generate_clicked = b1.button("Generate counts", type="primary")
submit_clicked = b2.button("Submit list request")

# Keep last results in session
if "last_df" not in st.session_state:
    st.session_state.last_df = None
if "last_params_codes" not in st.session_state:
    st.session_state.last_params_codes = None

# ---------- Generate counts ----------
if generate_clicked:
    try:
        df = filter_voters(params_codes)  # NOTE: pass mapped codes
        st.session_state.last_df = df
        st.session_state.last_params_codes = params_codes
    except Exception as e:
        st.error(f"Error generating counts: {e}")
        st.stop()

if st.session_state.last_df is not None:
    df = st.session_state.last_df
    people = int(len(df))
    households = compute_households(df)

    st.success(f"**# people:** {people:,}   •   **# households:** {households:,}")

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
            st.session_state.last_df = filter_voters(params_codes)
            st.session_state.last_params_codes = params_codes
        except Exception as e:
            st.error(f"Could not generate data for submission: {e}")
            st.stop()

    try:
        # Call your pipeline entrypoint instead of saving locally.
        # Signature:
        # generate_rct_mailing_list(
        #     list_df: pd.DataFrame,
        #     requestor_email: str,
        #     requestor_name: str,
        #     request_name: str,
        #     params
        # )
        generate_rct_mailing_list(
            list_df=st.session_state.last_df,
            requestor_email=st.session_state.user_info["email"],
            requestor_name=st.session_state.user_info["name"],
            request_name=safe_name,
            params=params,  # pass human-readable params along
        )
        st.success(f"List request **{safe_name}** submitted.")
        st.info(
            "You’ll see it appear in the Past lists table once your backend writes it to GCS."
        )
        # Soft refresh of the cached listing
        load_past_lists_gcs.clear()
    except NameError:
        st.error(
            "`generate_rct_mailing_list` is not defined/imported in this app. "
            "Import it or ensure it’s on PYTHONPATH."
        )
    except Exception as e:
        st.error(f"Failed to submit list request: {e}")

# =============== Footer ===============
st.markdown("---")
st.caption(
    "Notes: Past lists are read from GCS at gs://vr_mail_lists/lists/. "
    "Before querying, UI selections are mapped to codes (e.g., Male→M, Black→B). "
    "Replace the demo `filter_voters(params_codes)` with your real implementation."
)
