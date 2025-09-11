import pandas as pd
from enum import Enum


class SchemaFormat(Enum):
    BACKEND_DB = {
        "Party": "party",
        "Age": "age",
        "Gender": "gender",
        "Race": "race",
        "Ethnicity": "ethnicity",
        "County": "county",
        "CongressionalDistrict": "congressional_district",
        "StateSenateDistrict": "state_senate_district",
        "StateHouseDistrict": "state_house_district",
        "VR Program ID": "vr_program_id",
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

    def __init__(self):
        self.list_df = None

    def filter_voters(self, params, from_current_list=False):
        """Filters the existing list based on params. If from_current_list is False, it queries the database to create a new list. If it is true, it filters the existing list."""
        pass

    def get_person_list(self, schema_format: SchemaFormat):
        """returns a DataFrame at the person level in the specified schema format"""

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
