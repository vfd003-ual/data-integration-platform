from sql_server_connection_airflow import get_sql_server_connection
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Print environment variables (with password masked)
print("\nEnvironment variables:")
for var in ['SQL_SERVER_HOST', 'SQL_SERVER_DATABASE', 'SQL_SERVER_USERNAME']:
    print(f"{var}: {os.getenv(var)}")
print("SQL_SERVER_PASSWORD: ***********")

# Try to connect
conn = get_sql_server_connection()
if conn:
    print("\nConnection successful! Testing query...")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        print(f"\nSQL Server version: {row[0]}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"\nQuery error: {e}")
