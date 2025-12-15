# test_sheets_connection.py
import os
import sys

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
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
RANGE_NAME = 'Sheet1!A1:Z100'  # Adjust as needed

def test_connection():
    try:
        # Check credentials file exists
        if not os.path.exists(CREDENTIALS_FILE):
            print(f"❌ Credentials file not found: {CREDENTIALS_FILE}\n")
            print("Create a service account JSON from the Google Cloud Console, save it as the file above, and ensure it has Sheets API access.")
            print("Alternatively set the path via the CREDENTIALS_FILE variable in this script.")
            return
        # Authenticate
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
        
        # Build the service
        service = build('sheets', 'v4', credentials=creds)
        
        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print('No data found.')
        else:
            print(f'✅ Successfully connected to Google Sheets!')
            print(f'📊 Retrieved {len(values)} rows')
            print(f'First row: {values[0]}')
            
    except Exception as e:
        print(f'❌ Error: {e}')

if __name__ == '__main__':
    test_connection()