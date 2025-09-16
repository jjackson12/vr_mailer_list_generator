# Mail RCT Experiment List Generator Tool

This is a project developed to target lists of people for mailer campaigns, specifically for Randomized Control Trials (RCTs). The core functionality is the following:
1. **List Search:** Search a list of people based on age, race, gender, ethnicity, county, electoral districts, and party.
2. **List Details:** View list details including # people, # households, and basic distributions of key subgroups (as listed above)
3. **RCT Mailing List Generator:** Generate a randomized control group and associated mailing target list. These lists are then stored in Google Cloud and available for local download within the UI
4. **Power Analysis:** Perform basic Power Analysis for the RCT. By inputting some assumptions about your experiment, the tool suggests minimum sample size needed to expect to have statistically significant results from your experiment, both for the total population as well as subgroups of interest (e.g. if you wanted to target a whole state but also want measurement of impact specifically for Black voters to be part of your results)

### Cloud infrastructure and resources
Google Cloud was used for all cloud infrastructure for this project, with the exception of the web app being hosted on Streamlit. This tool specifically uses Google Cloud Bucket, Google BigQuery, and GMail API. These resources were constructed manually, but in a future iteration could be documented and generated using Terraform.

### List Search + Data Sourcing
This data is REAL and PUBLIC data sourced from the North Carolina State Board of Elections available here: https://dl.ncsbe.gov/

In the `scripts` subdirectory are basic python scripts I used to collect this data.

Also, I created a separate project to maintain a database of NCOA records that this app references to filter out addresses from output lists that have been marked as outdated. The project references the TrueNCOA API service. See that project here: https://github.com/jjackson12/ncoa-database-manager

### RCT Mailing List Generator
This generates a random sampling of people from the searched list to be a control group. This also creates four CSV files saved to Google Cloud Storage and available for download in the interface: person lists for both the treatment and control groups, and mailing lists (person lists aggregated to the household level) for both the treatment and control groups.

### Power Analysis
This uses the `statsmodels` package to conduct [Statistical Power evaluations for a z-test](https://www.statsmodels.org/dev/generated/statsmodels.stats.power.NormalIndPower.html). By inputting experiment design variables, the app tells you what necessary sample size is needed for a likely statistically significant result (power = .8, alpha = 0.05), both for the overall treatment group as well as for specificed subgroups for analysis.

## A Note on how "Production-Ready" I consider this app to be
**The short answer is "not very." I consider this somewhere past proof-of-concept but short of production ready.** The primary reason for that is a lack of proper testing. That would be necessary for long-term maintenance and also would most likely bring up some bugs for edge cases. Even somewhat basic things like retries-handling on API services used is not included. I also would want peer review of the statistical methods applied to the Power Analysis. There are certainly also more features one might want in a real-world application of this, such as multi-step searches, a proper review system for the data team to have a backend view on this, orchestration of the list generation process were it to become more complex, and so on. On a related note, [the NCOA database manager](https://github.com/jjackson12/ncoa-database-manager) is the furthest from production in that it is not yet orchestrated to run on something like Airflow to run regularly + by API request from this app. Another useful next step for this app would be to document/build the Google Cloud infrastructure using Terraform, and to properly dockerize the app as well.

## Development

You will need to create a file `.streamlit/secrets.toml` to store credentials for the Google BigQuery, Google Cloud Storage, and GMail API. If you wanted to rebuild this from scratch, you could copy my process loosely documented in `scripts/` to scrape your own voter file data from the NCBoE then create your own cloud infrastructure for the project.

After that, create a basic virtual environment and install requirements:
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

When debugging the Streamlit app, if using VSCode, add this to your configuration to run the debugger:
```
        {
            "name": "Python:Streamlit",
            "type": "debugpy",
            "request": "launch",
            "module": "streamlit",
            "env": {
                "PYDEVD_WARN_EVALUATION_TIMEOUT": "15"
            },
            "args": [
                 "run",
                 "streamlit_frontend.py",
                 "--server.port",
                 "8502"
            ]
        }
```

## Note on Usage of AI
I used Github Copilot significantly in building this app.
