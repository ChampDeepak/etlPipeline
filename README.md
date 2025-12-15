# etlPipeline

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

