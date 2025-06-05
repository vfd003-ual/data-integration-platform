import pandas as pd
import sys
from pathlib import Path
from sql_server_connection_airflow import get_sql_server_connection
from metrics_connection import ETLMetricsManager, get_metrics_connection

from datetime import datetime, timedelta
import os
import pytz
import re
import calendar

parent_dir = str(Path(__file__).resolve().parent)
sys.path.append(parent_dir)

# Configuración del archivo de log
ROOT_DIR = str(Path(__file__).resolve().parent)
LOG_DIR = os.path.join(ROOT_DIR, "logs")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Variable global para el timestamp de la ejecucion
EXECUTION_TIMESTAMP = None
LOG_FILE = None

def initialize_log():
    """Inicializa el archivo de log para esta ejecución"""
    global EXECUTION_TIMESTAMP, LOG_FILE
    # Obtener hora UTC y ajustar a hora local (+2 horas)
    utc_time = datetime.now(pytz.UTC)
    local_time = utc_time + timedelta(hours=2)
    EXECUTION_TIMESTAMP = local_time.strftime("%Y%m%d_%H%M%S")
    LOG_FILE = os.path.join(LOG_DIR, f"etl_log_{EXECUTION_TIMESTAMP}.txt")
    write_log("🚀 Iniciando nueva ejecución ETL")

def write_log(message):
    """Escribe el mensaje en el archivo de log y también lo muestra en consola"""
    utc_time = datetime.now(pytz.UTC)
    local_time = utc_time + timedelta(hours=2)
    timestamp = local_time.strftime("%Y%m%d_%H%M%S")
    log_message = f"[{timestamp}] {message}"
    
    # Escribir en archivo
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_message + '\n')
    
    # Mostrar en consola
    print(message)

def sustituir_nulls():
    write_log("Sustituyendo NULLs en DimProduct...")
    conn = get_sql_server_connection()
    metrics = ETLMetricsManager()
    metrics.start_process("sustituir_nulls", "DimProduct")

    if conn:
        try:
            cursor = conn.cursor()
            rows_updated = 0
            
            # Diccionario: columna -> valor de sustitucion
            columnas_a_actualizar = {
                "WeightUnitMeasureCode": "'Unk'",
                "SizeUnitMeasureCode": "'Unk'",
                "EnglishProductName": "'Unknown'",
                "SpanishProductName": "'Unknown'",
                "FrenchProductName": "'Unknown'",
                "StandardCost": "0",
                "FinishedGoodsFlag": "0",
                "Color": "'Unknown'",
                "SafetyStockLevel": "0",
                "ReorderPoint": "0",
                "ListPrice": "0",
                "Size": "'Unknown'",
                "SizeRange": "'Unknown'",
                "Weight": "0",
                "DaysToManufacture": "0",
                "ProductLine": "'Un'",
                "DealerPrice": "0",
                "Class": "'Un'",
                "Style": "'Un'",
                "ModelName": "'Unknown'",
                "LargePhoto": "NULL",  # tipo image o varbinary, mejor no modificarlo
                "EnglishDescription": "'Unknown'",
                "FrenchDescription": "'Unknown'",
                "ChineseDescription": "'Unknown'",
                "ArabicDescription": "'Unknown'",
                "HebrewDescription": "'Unknown'",
                "ThaiDescription": "'Unknown'",
                "GermanDescription": "'Unknown'",
                "JapaneseDescription": "'Unknown'",
                "TurkishDescription": "'Unknown'",
                "StartDate": "2013-07-02",  # ejemplo de fecha por defecto
                "EndDate": "2099-12-31",
                "Status": "'Unknown'"
            }

            for columna, valor in columnas_a_actualizar.items():
                if valor != "NULL":  # evitar modificar binarios u otros
                    update_query = f"""
                        UPDATE DimProduct
                        SET {columna} = {valor}
                        WHERE {columna} IS NULL
                    """
                    cursor.execute(update_query)
                    affected = cursor.rowcount
                    rows_updated += affected
                    metrics.add_processed_rows(affected)
                    conn.commit()
                    write_log(f"✅ NULLs sustituidos en columna {columna}.")
                else:
                    write_log(f"⚠️ Se ha omitido la columna {columna} por precaución.")
            
            write_log(f"✅ Todos los NULLs sustituidos correctamente. Filas actualizadas: {rows_updated}")
            write_log("💾 Guardando métricas del proceso de sustitución de NULLs...")
            metrics.save_metrics()
            write_log("✅ Métricas guardadas correctamente")

        except Exception as e:
            write_log(f"❌ Error al sustituir NULLs en DimProduct: {e}")
            metrics.add_error(str(e))
            write_log("💾 Guardando métricas del error...")
            metrics.save_metrics()
            write_log("✅ Métricas de error guardadas correctamente")
        finally:
            conn.close()

def validar_telefono(phone, postal_code=None, geography_key=None):
    """
    Valida y formatea número de teléfono según GeographyKey y código postal
    Args:
        phone: número de teléfono a validar
        postal_code: código postal para usar como código de área
        geography_key: clave de geografía del cliente
    Returns:
        Teléfono formateado o 'INVALID'
    """
    # Eliminar todo excepto números
    nums = re.sub(r'\D', '', str(phone))
    
    # Formato especial para GeographyKey 654: ###-555-####
    if geography_key == 654:
        if len(nums) >= 7:  # Asegurarnos de tener suficientes digitos
            return f"{nums[:3]}-555-{nums[-4:]}"
        return "INVALID"
    
    # Para otros casos, mantener la logica original
    if postal_code and len(nums) == 7:
        area_code = postal_code[:3]
        return f"+1 {area_code}-{nums[:3]}-{nums[3:]}"
    elif len(nums) == 10:
        return f"+1 {nums[:3]}-{nums[3:6]}-{nums[6:]}"
    else:
        return "INVALID"

def get_date_details(date_obj):
    """Genera los detalles de una fecha para DimDate"""
    # Reordenar los arrays para que coincidan con weekday() de Python (0=Lunes, 6=Domingo)
    days_en = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    days_es = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    days_fr = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
    
    months_en = list(calendar.month_name)[1:]
    months_es = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 
                'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
    months_fr = ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 'Juillet',
                'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre']

    weekday = date_obj.weekday()  # 0=Monday, 6=Sunday
    sql_weekday = {
        0: 2,  # Monday    -> 2
        1: 3,  # Tuesday   -> 3
        2: 4,  # Wednesday -> 4
        3: 5,  # Thursday  -> 5
        4: 6,  # Friday    -> 6
        5: 7,  # Saturday  -> 7
        6: 1   # Sunday    -> 1
    }[weekday]

    day_of_year = date_obj.timetuple().tm_yday
    week_of_year = date_obj.isocalendar()[1]
    month = date_obj.month
    quarter = ((month - 1) // 3) + 1
    semester = 1 if month <= 6 else 2

    return {
        'DateKey': int(date_obj.strftime('%Y%m%d')),
        'FullDateAlternateKey': date_obj.strftime('%Y-%m-%d'),
        'DayNumberOfWeek': sql_weekday,
        'EnglishDayNameOfWeek': days_en[weekday],
        'SpanishDayNameOfWeek': days_es[weekday],
        'FrenchDayNameOfWeek': days_fr[weekday],
        'DayNumberOfMonth': date_obj.day,
        'DayNumberOfYear': day_of_year,
        'WeekNumberOfYear': week_of_year,
        'EnglishMonthName': months_en[month-1],
        'SpanishMonthName': months_es[month-1],
        'FrenchMonthName': months_fr[month-1],
        'MonthNumberOfYear': month,
        'CalendarQuarter': quarter,
        'CalendarYear': date_obj.year,
        'CalendarSemester': semester,
        'FiscalQuarter': quarter,
        'FiscalYear': date_obj.year,
        'FiscalSemester': semester
    }

def insertar_fecha_en_dimension(conn, fecha):
    """Inserta una nueva fecha en DimDate"""
    try:
        date_obj = datetime.strptime(fecha, '%Y-%m-%d')
        date_details = get_date_details(date_obj)
        
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO DimDate (
                DateKey, FullDateAlternateKey, DayNumberOfWeek, 
                EnglishDayNameOfWeek, SpanishDayNameOfWeek, FrenchDayNameOfWeek,
                DayNumberOfMonth, DayNumberOfYear, WeekNumberOfYear,
                EnglishMonthName, SpanishMonthName, FrenchMonthName,
                MonthNumberOfYear, CalendarQuarter, CalendarYear,
                CalendarSemester, FiscalQuarter, FiscalYear, FiscalSemester
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, tuple(date_details.values()))
        
        conn.commit()
        write_log(f"✅ Nueva fecha insertada en DimDate: {fecha}")
        return date_details['DateKey']
        
    except Exception as e:
        write_log(f"❌ Error al insertar fecha en DimDate: {e}")
        return None

def verificar_fecha_en_dimension(conn, fecha):
    """Verifica si una fecha existe en DimDate y la inserta si no existe"""
    cursor = conn.cursor()
    query = """
        SELECT DateKey 
        FROM DimDate 
        WHERE FullDateAlternateKey = ?
    """
    cursor.execute(query, fecha)
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        write_log(f"⚠️ Fecha no encontrada en DimDate: {fecha}")
        return insertar_fecha_en_dimension(conn, fecha)

# Primero, definimos las GeographyKeys validas como constante
VALID_GEOGRAPHY_KEYS = {655, 654, 589, 483}

def validar_datos_cliente():
    """Valida y corrige datos de clientes"""
    write_log("🔍 Iniciando validación de datos de cliente...")
    metrics = ETLMetricsManager()
    metrics.start_process("validar_datos_cliente", "DimCustomer")
    
    try:
        conn = get_sql_server_connection()
        if not conn:
            write_log("❌ No se pudo establecer conexión con la base de datos")
            metrics.add_error("No se pudo establecer conexión con la base de datos")
            write_log("💾 Guardando métricas del error...")
            metrics.save_metrics()
            write_log("✅ Métricas de error guardadas correctamente")
            return
            
        cursor = conn.cursor()
        
        # Obtener todos los clientes que necesitan validacion
        query = """
            SELECT 
                c.CustomerKey,
                c.Phone,
                c.DateFirstPurchase,
                c.GeographyKey,
                g.PostalCode
            FROM DimCustomer c
            LEFT JOIN DimGeography g ON c.GeographyKey = g.GeographyKey
            WHERE (c.Phone IS NOT NULL AND c.GeographyKey IN ({valid_keys}))
                OR c.DateFirstPurchase IS NOT NULL
        """.format(valid_keys=','.join(map(str, VALID_GEOGRAPHY_KEYS)))
        
        cursor.execute(query)
        clientes = cursor.fetchall()
        write_log(f"📊 Procesando {len(clientes)} clientes...")
        total_updates = 0

        # Procesar cada cliente
        for cliente in clientes:
            customer_key, phone, date_first_purchase, geography_key, postal_code = cliente
            actualizaciones = []
            valores = []
            cambios_realizados = False

            # Validar telefono solo si el cliente esta en una geografia valida
            if phone and geography_key in VALID_GEOGRAPHY_KEYS:
                telefono_validado = validar_telefono(phone, postal_code, geography_key)
                if telefono_validado != phone and telefono_validado != "INVALID":
                    actualizaciones.append("Phone = ?")
                    valores.append(telefono_validado)
                    cambios_realizados = True
                    write_log(f"📱 Cliente {customer_key} (GeographyKey {geography_key}): Teléfono actualizado a {telefono_validado}")
            
            # Validar fecha solo si necesita ser insertada en DimDate
            if date_first_purchase:
                fecha_str = date_first_purchase.strftime('%Y-%m-%d')
                # Verificar si la fecha NO existe y necesita ser insertada
                cursor.execute("""
                    SELECT 1 
                    FROM DimDate 
                    WHERE FullDateAlternateKey = ?
                """, (fecha_str,))
                
                if not cursor.fetchone():  # Si la fecha no existe
                    if insertar_fecha_en_dimension(conn, fecha_str):
                        cambios_realizados = True
                        write_log(f"📅 Nueva fecha insertada para cliente {customer_key}: {fecha_str}")

            # Actualizar cliente si hay cambios
            if actualizaciones:
                valores.append(customer_key)
                update_query = f"""
                    UPDATE DimCustomer 
                    SET {', '.join(actualizaciones)}
                    WHERE CustomerKey = ?
                """
                cursor.execute(update_query, valores)
                conn.commit()
                
                # Solo incrementar contadores si hubo cambios reales
                if cambios_realizados:
                    total_updates += 1
                    metrics.add_processed_rows(1)
                    write_log(f"✅ Cliente {customer_key} actualizado con {len(actualizaciones)} cambios")

        write_log(f"✅ Validación de datos de clientes completada. Registros actualizados: {total_updates}")
        write_log("💾 Guardando métricas del proceso de validación de clientes...")
        metrics.save_metrics()
        write_log("✅ Métricas guardadas correctamente")
        
    except Exception as e:
        write_log(f"❌ Error en validación de datos: {str(e)}")
        write_log(f"🔍 Tipo de error: {type(e).__name__}")
        metrics.add_error(str(e))
        write_log("💾 Guardando métricas del error...")
        metrics.save_metrics()
        write_log("✅ Métricas de error guardadas correctamente")
        import traceback
        write_log(f"📜 Traceback completo:\n{traceback.format_exc()}")
    finally:
        if conn:
            conn.close()
            write_log("🔌 Conexión a base de datos cerrada")

def ejecutar_etl():
    """Ejecuta el proceso ETL completo"""
    try:
        initialize_log()  # Inicializar el log al comenzar
        write_log("🛠️ Ejecutando proceso ETL...")
        
        # Asegurar que la base de datos de métricas esté lista
        write_log("🔄 Verificando base de datos de métricas...")
        # Primero conectamos a master para crear la base de datos si no existe
        conn = get_metrics_connection(use_master=True)
        if not conn:
            write_log("❌ No se pudo establecer conexión con la base de datos master")
            return
        
        try:
            cursor = conn.cursor()
            
            # Asegurarnos de que no hay transacciones activas
            cursor.execute("SELECT @@TRANCOUNT")
            if cursor.fetchone()[0] > 0:
                write_log("🔄 Finalizando transacciones pendientes...")
                conn.commit()
            
            # Verificar si existe la base de datos
            write_log("🔄 Verificando si existe la base de datos ETLMetrics...")
            cursor.execute("SELECT database_id FROM sys.databases WHERE name = 'ETLMetrics'")
            if not cursor.fetchone():
                write_log("🔄 Creando base de datos ETLMetrics...")
                # Desactivar autocommit temporalmente para CREATE DATABASE
                conn.autocommit = True
                cursor.execute("CREATE DATABASE ETLMetrics")
                conn.autocommit = False
                write_log("✅ Base de datos ETLMetrics creada correctamente")
            else:
                write_log("ℹ️ La base de datos ETLMetrics ya existe")
            
            conn.close()

            # Ahora conectamos a la base de datos ETLMetrics
            write_log("🔄 Conectando a la base de datos ETLMetrics para crear las tablas...")
            conn = get_metrics_connection(use_master=False)
            if not conn:
                write_log("❌ No se pudo establecer conexión con la base de datos ETLMetrics")
                return

            cursor = conn.cursor()
            write_log("🔄 Verificando si existe la tabla de métricas...")
            # Crear tabla de metricas si no existe
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ETLMetrics')
            BEGIN
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
            END
            """)
            conn.commit()
            write_log("✅ Estructura de métricas verificada y lista")
            conn.close()

            # Sustituir NULLs
            write_log("📋 Iniciando sustitución de NULLs...")
            sustituir_nulls()
            write_log("✅ Proceso de sustitución de NULLs completado")
            
            # Validar datos de cliente
            write_log("👤 Iniciando validación de datos de cliente...")
            validar_datos_cliente()
            write_log("✅ Proceso de validación de clientes completado")
            
            write_log("✅ ETL ejecutado con éxito.")
            
        except Exception as e:
            write_log(f"❌ Error en la configuración de la base de datos de métricas: {str(e)}")
            write_log(f"🔍 Tipo de error: {type(e).__name__}")
            import traceback
            write_log(f"📜 Traceback completo:\n{traceback.format_exc()}")
            
    except Exception as e:
        write_log(f"❌ Error en la ejecución del ETL: {str(e)}")
        write_log(f"🔍 Tipo de error: {type(e).__name__}")
        import traceback
        write_log(f"📜 Traceback completo:\n{traceback.format_exc()}")

if __name__ == "__main__":
    ejecutar_etl()