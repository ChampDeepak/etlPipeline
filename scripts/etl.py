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

# Google Sheets Config
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT_FILE = 'google-sheets-credentials.json' # This file will be created by GitHub Actions
SPREADSHEET_ID = '1bQJwagSURpl2vcN3RUdUwDDK3zjhXtPLKX2kYO_O4HU' 
RANGE_NAME = 'netflix!A:L' 

# Database Connection
load_dotenv()

# PRIORITY: Try to get URL from Environment Variable (GitHub Actions), 
# otherwise fall back to your local testing string.
DATABASE_URL = os.getenv('DATABASE_URL') 

if not DATABASE_URL:
    # Fallback for local testing if .env is missing
    DATABASE_URL = "postgres://neon:npg@localhost:5431/netflix"

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def get_safe_items(value):
    """
    Safely splits a comma-separated string into a list.
    Returns an empty list if the value is None, NaN, or empty.
    """
    if pd.isna(value) or not value:
        return []
    return [item.strip() for item in str(value).split(',')]

def safe_extract_unique(series):
    """
    Extracts unique values from a column that contains comma-separated strings.
    Handles NaNs and empty strings safely.
    """
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
            return None
            
        headers = values[0]
        data = values[1:]
        df = pd.DataFrame(data, columns=headers)
        print(f"✅ Extracted {len(df)} rows.")
        return df
    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        return None

def transform_data(df):
    print("⚙️ TRANSFORM: Cleaning and Normalizing data...")
    # Replace empty strings with None
    df = df.replace('', None)
    
    # Fix Date (errors='coerce' produces NaT for invalid dates)
    df['date_added'] = pd.to_datetime(df['date_added'].str.strip(), errors='coerce').dt.date
    
    # Fix Rating vs Duration shift
    mask = df['rating'].str.contains('min', na=False)
    df.loc[mask, 'duration'] = df.loc[mask, 'rating']
    df.loc[mask, 'rating'] = None
    
    # Fix Year (errors='coerce' produces NaN for invalid numbers)
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
    
    # --- CRITICAL FIX: Convert Pandas NaT/NaN to Python None for SQL ---
    # We cast to object to ensure integers/floats can hold 'None'
    df = df.astype(object).where(pd.notnull(df), None)
    
    return df

def load_to_db(df):
    print("💾 LOAD: Connecting to Database...")
    conn = None
    try:
        conn = psycopg2.connect(dsn=DATABASE_URL)
        conn.autocommit = False
        cur = conn.cursor()

        print("   ...Starting optimized batch loading...")

        # --- A. Prepare Data Structures ---
        
        # 1. Main Shows Data
        shows_to_insert = df[[
            'show_id', 'type', 'title', 'date_added', 
            'release_year', 'rating', 'duration', 'description'
        ]].values.tolist()

        # 2. Reference Data (Raw Unique Values)
        # Structure: (Unique Values, Column Name, ID Column, Junction Table Name)
        ref_data = {
            'directors': (safe_extract_unique(df['director']), 'director_name', 'director_id', 'show_directors'),
            'actors':    (safe_extract_unique(df['cast_members']), 'actor_name', 'actor_id', 'show_cast'),
            'countries': (safe_extract_unique(df['country']), 'country_name', 'country_id', 'show_countries'),
            'genres':    (safe_extract_unique(df['listed_in']), 'genre_name', 'genre_id', 'show_genres')
        }

        # --- B. Insert Reference Data & Build ID Maps ---
        id_maps = {} 

        for table, (unique_values, col_name, id_col, _) in ref_data.items():
            # Filter valid strings
            insert_values = [(v,) for v in unique_values if v and len(v) > 0]
            
            if insert_values:
                print(f"   - Inserting {len(insert_values)} unique records into {table}...")
                insert_sql = f"INSERT INTO {table} ({col_name}) VALUES %s ON CONFLICT ({col_name}) DO NOTHING"
                extras.execute_values(cur, insert_sql, insert_values)
            
            # Fetch IDs back to Python
            cur.execute(f"SELECT {col_name}, {id_col} FROM {table}")
            id_maps[table] = {name: id for name, id in cur.fetchall()}

        # --- C. Map Relationships (Junction Data) ---
        junction_data = {
            'show_directors': [], 
            'show_cast': [], 
            'show_countries': [], 
            'show_genres': []
        }

        for _, row in df.iterrows():
            show_id = row['show_id']
            
            # Map Directors
            for item in get_safe_items(row['director']):
                if item in id_maps['directors']:
                    junction_data['show_directors'].append((show_id, id_maps['directors'][item]))

            # Map Cast
            for item in get_safe_items(row['cast_members']):
                if item in id_maps['actors']:
                    junction_data['show_cast'].append((show_id, id_maps['actors'][item]))

            # Map Countries
            for item in get_safe_items(row['country']):
                if item in id_maps['countries']:
                    junction_data['show_countries'].append((show_id, id_maps['countries'][item]))

            # Map Genres
            for item in get_safe_items(row['listed_in']):
                if item in id_maps['genres']:
                    junction_data['show_genres'].append((show_id, id_maps['genres'][item]))

        # --- D. Final Batch Inserts ---

        # 1. Insert Shows
        print(f"   - Inserting {len(shows_to_insert)} records into shows...")
        shows_sql = """
            INSERT INTO shows (show_id, type, title, date_added, release_year, rating, duration, description)
            VALUES %s ON CONFLICT (show_id) DO NOTHING
        """
        extras.execute_values(cur, shows_sql, shows_to_insert)

        # 2. Insert Junctions
        # Map Junction Table Name -> Target ID Column Name
        fk_map = {
            'show_directors': 'director_id',
            'show_cast': 'actor_id',      
            'show_countries': 'country_id',
            'show_genres': 'genre_id'
        }

        for table, data_list in junction_data.items():
            if data_list:
                print(f"   - Inserting {len(data_list)} records into {table}...")
                col_fk = fk_map[table]
                junction_sql = f"INSERT INTO {table} (show_id, {col_fk}) VALUES %s ON CONFLICT DO NOTHING"
                extras.execute_values(cur, junction_sql, data_list)

        conn.commit()
        print("\n✅ ETL Job Completed Successfully!")

    except Exception as e:
        if conn: conn.rollback()
        print(f"\n❌ Error during loading: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn: conn.close()

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    print("=" * 50)
    print("🔄 ETL Pipeline Started")
    print("=" * 50)
    
    df_raw = extract_from_sheets()
    if df_raw is not None:
        df_clean = transform_data(df_raw)
        load_to_db(df_clean)
    else:
        print("⚠️  No data extracted.")