# vr_mailer_list_generator

This is a scrappy sample project meant to build out two primary tools:
1. Maintain a database in BigQuery that tracks a person / voter file list, and regularly updates it with new NCOA data queried via API.
2. A tool that allows a non-technical user to build basic queries of people in the person / voter file database and submit and track requests to generate lists of mailer addresses for these people.



## Development
When debugging the Streamlit app, if using VSCode, add this to your configuration:
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
