import psycopg2
from psycopg2 import OperationalError
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('../config/.env')

def test_neon_connection():
    """
    Test connection to NeonDB using connection string
    """
    try:
        # Get connection string from environment variable
        connection_string = os.getenv('DATABASE_URL', '')
        
        if not connection_string:
            print("❌ No DATABASE_URL found in .env file")
            print("💡 Add your connection string to .env file:")
            print("   DATABASE_URL=postgresql://user:password@host:port/dbname")
            return False
        
        print("🔄 Attempting to connect to NeonDB...")
        print(f"   Connection String: {connection_string[:30]}...") # Show partial for security
        print("-" * 50)
        
        # Establish connection using connection string
        connection = psycopg2.connect(connection_string)
        
        # Create a cursor object
        cursor = connection.cursor()
        
        # Execute a test query
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        
        # Execute additional test queries
        cursor.execute("SELECT current_database();")
        current_db = cursor.fetchone()
        
        cursor.execute("SELECT current_user;")
        current_user = cursor.fetchone()
        
        # Print success message
        print("✅ Connected to NeonDB successfully!")
        print("-" * 50)
        print(f"📊 Database Version:\n   {db_version[0]}")
        print("-" * 50)
        print(f"🗄️  Current Database: {current_db[0]}")
        print(f"👤 Current User: {current_user[0]}")
        print("-" * 50)
        
        # Test creating a simple table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS connection_test (
                id SERIAL PRIMARY KEY,
                test_message VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Insert test data
        cursor.execute("""
            INSERT INTO connection_test (test_message) 
            VALUES ('Connection test successful!');
        """)
        
        # Commit the transaction
        connection.commit()
        
        # Verify the insert
        cursor.execute("SELECT * FROM connection_test ORDER BY id DESC LIMIT 1;")
        test_record = cursor.fetchone()
        print(f"✅ Test table created and data inserted successfully!")
        print(f"   Test Record: {test_record}")
        print("-" * 50)
        
        # Clean up test table (optional)
        cursor.execute("DROP TABLE IF EXISTS connection_test;")
        connection.commit()
        print("🧹 Test table cleaned up")
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        print("\n🎉 All connection tests passed successfully!")
        print("\n" + "=" * 50)
        print("✅ DATABASE CONNECTION ESTABLISHED")
        print("=" * 50)
        
        return True
        
    except OperationalError as e:
        print(f"❌ Connection Error: {e}")
        print("\n💡 Troubleshooting tips:")
        print("   1. Ensure Neon local extension is running")
        print("   2. Check if the connection string is correct")
        print("   3. Verify DATABASE_URL in .env file")
        print("   4. Confirm the database exists and is accessible")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("🚀 NeonDB Connection Test Script")
    print("=" * 50)
    test_neon_connection()