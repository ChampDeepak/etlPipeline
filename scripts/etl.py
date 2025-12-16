import os
import sys
import pandas as pd
import psycopg2
from psycopg2 import extras
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

# ==========================================
# 1. CONFIGURATION
# ==========================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(PROJECT_ROOT, 'config')

# Note: Removed '.readonly' to allow writing back to the sheet
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = os.path.join(CONFIG_DIR, 'google-sheets-credentials.json')
SPREADSHEET_ID = '1bQJwagSURpl2vcN3RUdUwDDK3zjhXtPLKX2kYO_O4HU' 
# Extended range to Column M to capture Validation_Status
RANGE_NAME = 'netflix!A:M' 

load_dotenv(os.path.join(CONFIG_DIR, '.env'))
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    DATABASE_URL = "postgres://neon:npg@localhost:5431/netflix"

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def get_safe_items(value):
    if pd.isna(value) or not value:
        return []
    return [item.strip() for item in str(value).split(',')]

def safe_extract_unique(series):
    return series.dropna().astype(str).str.split(',').explode().str.strip().unique()

# ==========================================
# 3. ETL PIPELINE STEPS
# ==========================================

def extract_from_sheets():
    print("🚀 EXTRACT: Connecting to Google Sheets...")
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        
        if not values:
            return None, None
            
        headers = values[0]
        data = values[1:]
        df = pd.DataFrame(data, columns=headers)
        
        # Add original row numbers (Header is row 1, data starts row 2)
        # We need this to update the specific cell back in Google Sheets
        df['sheet_row_num'] = range(2, 2 + len(df))
        
        print(f"✅ Extracted {len(df)} total rows from Sheets.")
        return df, service
    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        return None, None

def transform_data(df):
    print("⚙️ TRANSFORM: Cleaning and Normalizing data...")
    df = df.replace('', None)
    df['date_added'] = pd.to_datetime(df['date_added'].str.strip(), errors='coerce').dt.date
    
    mask = df['rating'].str.contains('min', na=False)
    df.loc[mask, 'duration'] = df.loc[mask, 'rating']
    df.loc[mask, 'rating'] = None
    
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
    df = df.astype(object).where(pd.notnull(df), None)
    return df

def load_to_db(df):
    print(f"💾 LOAD: Inserting {len(df)} new rows into Database...")
    conn = None
    try:
        conn = psycopg2.connect(dsn=DATABASE_URL)
        conn.autocommit = False
        cur = conn.cursor()

        # 1. Main Shows Data
        shows_to_insert = df[[
            'show_id', 'type', 'title', 'date_added', 
            'release_year', 'rating', 'duration', 'description'
        ]].values.tolist()

        # 2. Reference Data extraction (Same as before)
        ref_data = {
            'directors': (safe_extract_unique(df['director']), 'director_name', 'director_id', 'show_directors'),
            'actors':    (safe_extract_unique(df['cast_members']), 'actor_name', 'actor_id', 'show_cast'),
            'countries': (safe_extract_unique(df['country']), 'country_name', 'country_id', 'show_countries'),
            'genres':    (safe_extract_unique(df['listed_in']), 'genre_name', 'genre_id', 'show_genres')
        }

        # 3. Reference Insert
        id_maps = {} 
        for table, (unique_values, col_name, id_col, _) in ref_data.items():
            insert_values = [(v,) for v in unique_values if v and len(v) > 0]
            if insert_values:
                insert_sql = f"INSERT INTO {table} ({col_name}) VALUES %s ON CONFLICT ({col_name}) DO NOTHING"
                extras.execute_values(cur, insert_sql, insert_values)
            cur.execute(f"SELECT {col_name}, {id_col} FROM {table}")
            id_maps[table] = {name: id for name, id in cur.fetchall()}

        # 4. Map Junctions
        junction_data = {'show_directors': [], 'show_cast': [], 'show_countries': [], 'show_genres': []}

        for _, row in df.iterrows():
            show_id = row['show_id']
            for item in get_safe_items(row['director']):
                if item in id_maps['directors']: junction_data['show_directors'].append((show_id, id_maps['directors'][item]))
            for item in get_safe_items(row['cast_members']):
                if item in id_maps['actors']: junction_data['show_cast'].append((show_id, id_maps['actors'][item]))
            for item in get_safe_items(row['country']):
                if item in id_maps['countries']: junction_data['show_countries'].append((show_id, id_maps['countries'][item]))
            for item in get_safe_items(row['listed_in']):
                if item in id_maps['genres']: junction_data['show_genres'].append((show_id, id_maps['genres'][item]))

        # 5. Insert Shows
        shows_sql = """
            INSERT INTO shows (show_id, type, title, date_added, release_year, rating, duration, description)
            VALUES %s ON CONFLICT (show_id) DO NOTHING
        """
        extras.execute_values(cur, shows_sql, shows_to_insert)

        # 6. Insert Junctions
        fk_map = {'show_directors': 'director_id', 'show_cast': 'actor_id', 'show_countries': 'country_id', 'show_genres': 'genre_id'}
        for table, data_list in junction_data.items():
            if data_list:
                col_fk = fk_map[table]
                junction_sql = f"INSERT INTO {table} (show_id, {col_fk}) VALUES %s ON CONFLICT DO NOTHING"
                extras.execute_values(cur, junction_sql, data_list)

        conn.commit()
        print("✅ DB Load Successful.")
        return True # Return success flag

    except Exception as e:
        if conn: conn.rollback()
        print(f"❌ Error during loading: {e}")
        return False
    finally:
        if conn: conn.close()

# ==========================================
# 4. MARK PROCESSED ROWS (Write Back)
# ==========================================
def mark_rows_as_added(service, rows_to_update):
    print("📝 UPDATING SHEETS: Marking rows as '🚀 ADDED'...")
    
    data = []
    # Prepare batch update
    for row_num in rows_to_update:
        data.append({
            "range": f"netflix!M{row_num}", # Update Column M (Status)
            "values": [["🚀 ADDED"]]
        })

    body = {
        "valueInputOption": "USER_ENTERED",
        "data": data
    }
    
    try:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, 
            body=body
        ).execute()
        print(f"✅ Successfully updated {len(data)} rows in Google Sheets.")
    except Exception as e:
        print(f"❌ Failed to update sheet status: {e}")

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    print("=" * 50)
    print("🔄 ETL Pipeline Started (Incremental Mode)")
    print("=" * 50)
    
    # 1. Extract Full Data
    df_all, service = extract_from_sheets()
    
    if df_all is not None:
        # 2. FILTER: Only process rows that are '✅ READY'
        # Ensure Column M (Validation_Status) exists
        if 'Validation_Status' in df_all.columns:
            df_new = df_all[df_all['Validation_Status'] == '✅ READY'].copy()
        else:
            print("⚠️ 'Validation_Status' column not found. Processing ALL rows (Fallback).")
            df_new = df_all

        if df_new.empty:
            print("✨ No new '✅ READY' rows found. System is up to date.")
            sys.exit(0)

        print(f"🔍 Found {len(df_new)} new rows to process.")

        # 3. Transform
        df_clean = transform_data(df_new)
        
        # 4. Load
        success = load_to_db(df_clean)
        
        # 5. Write Back (Only if DB load was successful)
        if success:
            mark_rows_as_added(service, df_new['sheet_row_num'])
            
    else:
        print("⚠️ No data extracted.")