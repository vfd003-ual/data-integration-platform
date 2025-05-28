import pyodbc
import os
from dotenv import load_dotenv
import platform

# Load environment variables
load_dotenv()

def get_sql_server_connection():
    server = os.getenv('SQL_SERVER_HOST')
    database = os.getenv('SQL_SERVER_DATABASE')
    username = os.getenv('SQL_SERVER_USERNAME')
    password = os.getenv('SQL_SERVER_PASSWORD')
    
    # Use different driver names based on the environment
    if platform.system() == 'Windows':
        driver = '{ODBC Driver 17 for SQL Server}'
    else:
        driver = 'ODBC Driver 17 for SQL Server'  # Linux format

    try:
        conn = pyodbc.connect(
            f"DRIVER={driver};SERVER={server},1433;DATABASE={database};UID={username};PWD={password};Encrypt=no"
        )
        print("✅ Conexión a SQL Server exitosa")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a SQL Server: {e}")
        return None
