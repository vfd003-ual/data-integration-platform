import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_sql_server_connection():
    server = os.getenv('SQL_SERVER_LOCAL_SERVER')
    database = os.getenv('SQL_SERVER_LOCAL_DATABASE')
    driver = os.getenv('SQL_SERVER_LOCAL_DRIVER')
    try:
        conn = pyodbc.connect(
            f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
        print("✅ Conexión a SQL Server exitosa")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a SQL Server: {e}")
        return None
