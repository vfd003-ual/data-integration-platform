from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging

# Configuración predeterminada del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Ruta del archivo dentro del directorio montado en /opt/airflow/external_files
ARCHIVO_LOG = "/opt/airflow/external_files/resultado_fuera_de_airflow.txt"

# Definición del DAG
dag = DAG(
    'procesar_transaccion_escrita_dag',
    default_args=default_args,
    description='Procesa transacciones y escribe en un archivo fuera de Airflow',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def escribir_mensaje_archivo(mensaje):
    """Escribe un mensaje en el archivo del directorio mapeado."""
    try:
        with open(ARCHIVO_LOG, 'a', encoding='utf-8') as f:
            f.write(mensaje + '\n')
    except Exception as e:
        logging.error(f"Error escribiendo en archivo: {e}")

def mensaje_1(**context):
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    texto = f"MENSAJE 1: Operación realizada correctamente fuera de Airflow. Fecha y hora: {ahora}"
    print(texto)
    logging.info(texto)
    escribir_mensaje_archivo(texto)
    return True

# Definición de tareas
tarea_1 = PythonOperator(
    task_id='mensaje_1',
    python_callable=mensaje_1,
    provide_context=True,
    dag=dag,
)

# Ejecutar la tarea
tarea_1
