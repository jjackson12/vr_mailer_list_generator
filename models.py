import pandas as pd
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SchemaFormat(Enum):
    BIGQUERY = {
        "Party": "party_cd",
        "Age": "age_at_year_end",
        "Gender": "gender_code",
        "Race": "race_code",
        "Ethnicity": "ethnic_code",
        "County": "county_desc",
        "CongressionalDistrict": "cong_dist_abbrv",
        "StateSenateDistrict": "nc_senate_abbrv",
        "StateHouseDistrict": "nc_house_abbrv",
    }
    TRUENCOA = {
        "Party": "PartyAffiliation",
        "Age": "VoterAge",
        "Gender": "VoterGender",
        "Race": "VoterRace",
        "Ethnicity": "VoterEthnicity",
        "County": "CountyName",
        "CongressionalDistrict": "CongressionalDist",
        "StateSenateDistrict": "StateSenateDist",
        "StateHouseDistrict": "StateHouseDist",
        "VR Program ID": "VRProgramIdentifier",
    }
    DEFAULT = {
        "Party": "Party",
        "Age": "Age",
        "Gender": "Gender",
        "Race": "Race",
        "Ethnicity": "Ethnicity",
        "County": "County",
        "CongressionalDistrict": "CongressionalDistrict",
        "StateSenateDistrict": "StateSenateDistrict",
        "StateHouseDistrict": "StateHouseDistrict",
        "VR Program ID": "VR Program ID",
    }


class PersonList:

    PERSON_VARIABLES = [
        "Party",
        "Age",
        "Gender",
        "Race",
        "Ethnicity",
        "County",
        "CongressionalDistrict",
        "StateSenateDistrict",
        "StateHouseDistrict",
        "VR Program ID",
    ]

    def __init__(self, bq_client):
        self.list_df = None
        self.bq_client = bq_client

    def filter_voters(self, params, from_current_list=False):
        """Filters the existing list based on params. If from_current_list is False, it queries the database to create a new list. If it is true, it filters the existing list."""

        logger.info("Querying BigQuery for voter data")
        # Remove parameters with empty lists or None values
        params = {k: v for k, v in params.items() if v not in (None, [])}

        # Ensure that params keys are valid
        #  TODO: How do we know the formatting of the Params is correct? Where does the SchemaFormat come in?
        invalid_params = [k for k in params.keys() if k not in self.PERSON_VARIABLES]
        if len(invalid_params) > 0:
            raise ValueError(f"One or more parameters are not valid: {invalid_params}")

        # Base query
        query = "SELECT * FROM `vr-mail-generator.voterfile.vf_nc_partial` WHERE 1=1"
        # Mutate columns to match parameters

        special_queries = {
            "Age": lambda x: f" AND age_at_year_end BETWEEN {min(x)} AND {max(x)}"
        }
        int_fields = [
            "StateSenateDistrict",
            "StateHouseDistrict",
            "CongressionalDistrict",
        ]
        # Add filters dynamically based on params
        for key, value in params.items():
            if value is not None:
                # TODO: Test
                param_db_name = SchemaFormat.BIGQUERY[key]
                if key in special_queries:
                    query += special_queries[key](value)
                else:
                    if isinstance(value, list):
                        if key in int_fields:
                            value_list = ", ".join([str(int(v)) for v in value])
                        else:
                            value_list = ", ".join([f"'{v}'" for v in value])
                        query += f" AND {param_db_name} IN ({value_list})"
                    else:
                        query += f" AND {param_db_name} = '{value}'"

        logger.debug(f"Constructed query: {query}")

        # Execute query
        query_job = self.bq_client.query(query)
        result = query_job.result()
        self.list_df = result.to_dataframe()

        logger.info(f"Query complete: rows fetched={len(self.list_df)}")

    def get_person_list(self, schema_format: SchemaFormat = SchemaFormat.DEFAULT):
        """returns a DataFrame at the person level in the specified schema format"""
        if self.list_df is None:
            raise ValueError("No list loaded. Please load or create a list first.")
        # Rename columns based on the specified schema format
        rename_map = {v: k for k, v in schema_format.value.items()}
        person_df = self.list_df.rename(columns=rename_map)
        # TODO: When might we be filtering out columns here? Should prob be a warning if we do
        return person_df[list(rename_map.keys())]

    def get_household_list(self, schema_format: SchemaFormat):
        """returns a DataFrame in the specified schema format, aggregating persons into households"""

    def get_variable_options(self, variable_name):
        """returns a list of unique values for the specified variable in the current list"""
        # TODO: Make sure this is the best way to do this, rather than querying BigQuery. I think the only time that that would be better would be when we're referencing the entire database rather than a filtered list.
        if self.list_df is None:
            raise ValueError("No list loaded. Please load or create a list first.")
        if variable_name not in self.PERSON_VARIABLES:
            raise ValueError(f"Variable '{variable_name}' is not recognized.")
        return (
            self.list_df[SchemaFormat.BACKEND_DB.value[variable_name]]
            .dropna()
            .unique()
            .tolist()
        )
