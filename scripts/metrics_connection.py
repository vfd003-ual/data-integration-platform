import pyodbc
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import platform

# Load environment variables
load_dotenv()

def get_metrics_connection(use_master=False):
    server = os.getenv('SQL_SERVER_HOST')
    database = 'master' if use_master else 'ETLMetrics'
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
        print(f"✅ Conexión a base de datos {database} exitosa")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a base de datos de métricas: {e}")
        return None

def ensure_metrics_tables_exist():
    """Asegura que las tablas de métricas existan, creándolas si es necesario."""
    conn = get_metrics_connection()
    if not conn:
        return False
        
    try:
        cursor = conn.cursor()
        
        # Crear tabla de metricas ETL si no existe
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ETLMetrics')
        CREATE TABLE ETLMetrics (
            MetricID INT IDENTITY(1,1) PRIMARY KEY,
            ExecutionTimestamp DATETIME NOT NULL,
            ProcessName VARCHAR(100) NOT NULL,
            TableAffected VARCHAR(100) NOT NULL,
            RowsProcessed INT NOT NULL,
            ErrorCount INT NOT NULL,
            ExecutionTimeSeconds FLOAT NOT NULL,
            AdditionalInfo NVARCHAR(MAX)
        )
        """)
        
        conn.commit()
        print("✅ Tablas de métricas verificadas/creadas correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error al crear tablas de métricas: {e}")
        return False
    finally:
        conn.close()

class ETLMetricsManager:
    def __init__(self):
        self.start_time = None
        self.process_name = None
        self.table_affected = None
        self.rows_processed = 0
        self.error_count = 0
        self.additional_info = None

    def start_process(self, process_name, table_affected):
        self.start_time = datetime.now() + timedelta(hours=2)
        self.process_name = process_name
        self.table_affected = table_affected
        self.rows_processed = 0
        self.error_count = 0
        self.additional_info = None

    def add_processed_rows(self, count):
        self.rows_processed += count

    def add_error(self, error_message=None):
        self.error_count += 1
        if error_message:
            if self.additional_info:
                self.additional_info += f"\n{error_message}"
            else:
                self.additional_info = error_message

    def save_metrics(self):
        if not self.start_time:
            raise ValueError("No se ha iniciado ningún proceso de ETL")

        # Calculamos el tiempo de ejecucion usando tambien el timestamp ajustado
        execution_time = ((datetime.now() + timedelta(hours=2)) - self.start_time).total_seconds()
        
        try:
            conn = get_metrics_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO ETLMetrics (
                        ExecutionTimestamp, ProcessName, TableAffected,
                        RowsProcessed, ErrorCount, ExecutionTimeSeconds, AdditionalInfo
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.start_time,
                    self.process_name,
                    self.table_affected,
                    self.rows_processed,
                    self.error_count,
                    execution_time,
                    self.additional_info
                ))
                conn.commit()
                conn.close()
                print(f"✅ Métricas guardadas para {self.process_name}")
        except Exception as e:
            print(f"❌ Error al guardar métricas: {e}")
