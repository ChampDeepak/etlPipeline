# etlPipeline

## Project Discription
This project provides a system that acts as a bridge between Google Sheets and Robust PostgreSql DB. If a new row is added in the Google Sheet, the system will validate the data and then add that Data in PSQL DB. This system can we used in a case where class instructor is taking the attendance on Google Sheet but want to automate that attendance data migration to PSQL DB. This is just one use case example, this system can used any scenario where we want to automate new data migration from Google Sheets to PSQL DB. 

## I learned following things through this project: 
1. ETL Pipeline Design
2. Google App Scripts
3. Github Actions
4. psycopg2 (Python module to intract with PSQL DB)
5. Google Sheets API

## Setup

1. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
```

3. Google Sheets access (if you use `test_sheets_connection.py`):

- Create a service account in Google Cloud Console
- Enable the Google Sheets API and download the service account JSON
- Save it as `google-sheets-credentials.json` at repo root (or update `CREDENTIALS_FILE` in the script)

Run the test script:

```bash
python test_sheets_connection.py
```

If the script reports "Credentials file not found", make sure your JSON key file is present and the Sheets API is enabled for the project.

