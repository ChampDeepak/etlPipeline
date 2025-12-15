# test_sheets_connection.py
# python test_sheets_connection.py --range 'netflix!A1:A1'
import os
import sys
import json
import argparse

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError as e:
    print("\n❌ Missing Python dependency for Google APIs:")
    print(f"   {e}\n")
    print("Tip: install required packages with:\n    python -m pip install -r requirements.txt\n")
    sys.exit(1)

# Path to your downloaded JSON credentials
CREDENTIALS_FILE = 'google-sheets-credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Your Google Sheet ID (from the URL)
SPREADSHEET_ID = '1bQJwagSURpl2vcN3RUdUwDDK3zjhXtPLKX2kYO_O4HU'  # Get this from your Sheet URL
RANGE_NAME = 'netflix!A1:A1'  # Adjust as needed


def parse_args():
    parser = argparse.ArgumentParser(description='Test Google Sheets connection')
    parser.add_argument('--creds', '-c', default=CREDENTIALS_FILE, help='Path to service account JSON')
    parser.add_argument('--sheet', '-s', default=SPREADSHEET_ID, help='Spreadsheet ID')
    parser.add_argument('--range', '-r', dest='range_name', default=RANGE_NAME, help='A1 range or named range')
    return parser.parse_args()

def test_connection(creds_path=CREDENTIALS_FILE, sheet_id=SPREADSHEET_ID, range_name=RANGE_NAME):
    try:
        # Check credentials file exists
        if not os.path.exists(creds_path):
            print(f"❌ Credentials file not found: {creds_path}\n")
            print("Create a service account JSON from the Google Cloud Console, save it as the file above, and ensure it has Sheets API access.")
            print("Alternatively set the path via the --creds/-c argument.")
            return

        # Try to show the service account email from the JSON for clarity
        try:
            with open(creds_path, 'r', encoding='utf-8') as fh:
                jd = json.load(fh)
                client_email = jd.get('client_email')
                if client_email:
                    print(f"Service account: {client_email}")
        except Exception:
            client_email = None

        # Authenticate
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)

        # Build the service
        service = build('sheets', 'v4', credentials=creds)

        # Show available sheet titles (helpful for debugging ranges)
        try:
            meta = service.spreadsheets().get(spreadsheetId=sheet_id, fields='sheets.properties.title').execute()
            sheets = [s['properties']['title'] for s in meta.get('sheets', [])]
            print(f"Spreadsheet contains sheets: {sheets}")
        except HttpError as e:
            print(f"❌ Unable to fetch spreadsheet metadata: {e}\n")
            print("Possible causes: spreadsheet ID wrong, service account doesn't have access, or Sheets API not enabled.")
            if client_email:
                print(f"If needed, share the sheet with: {client_email}")
            return

        # Call the Sheets API for the provided range
        print(f"Requesting values for range: {range_name!r} (spreadsheetId={sheet_id})")
        # (This prints exactly what will be sent to the API so you can verify it matches your expectation)
        try:
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=sheet_id, range=range_name).execute()
            values = result.get('values', [])
            if not values:
                print('No data found in the requested range.')
            else:
                print(f'✅ Successfully connected to Google Sheets!')
                print(f'📊 Retrieved {len(values)} rows')
                # Show the first few rows with their indices so it's clear what 'values' contains
                for i, row in enumerate(values[:5]):
                    print(f'Row {i}: {row}')
                # Helpful note: values is a list of rows; to access the second cell of the first row use values[0][1]
        except HttpError as e:
            # Provide more context for range parsing / permission errors
            msg = getattr(e, 'error_details', None) or str(e)
            print(f"❌ Sheets API error: {msg}\n")
            if 'Unable to parse range' in str(e):
                print('It looks like the requested range could not be parsed by the Sheets API.')
                print('Suggestions:')
                print(" - Check the sheet name exists (see 'Spreadsheet contains sheets' above)")
                print(" - Try a simpler range like 'A1' or 'Sheet1!A1:A1'")
            if client_email:
                print(f"Ensure the sheet is shared with the service account: {client_email}")
            return
    except Exception as e:
        print(f'❌ Unexpected error: {e}')

if __name__ == '__main__':
    args = parse_args()
    creds_path = args.creds
    sheet_id = args.sheet
    range_name = args.range_name
    test_connection(creds_path=creds_path, sheet_id=sheet_id, range_name=range_name)