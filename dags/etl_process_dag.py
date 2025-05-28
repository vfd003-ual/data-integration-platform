from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Configuración predeterminada del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'etl_process_dag',
    description='Ejecutar procesos ETL desde un script local',
    schedule_interval=None,  # Solo se ejecuta manualmente
    start_date=datetime(2025, 4, 28),  # Fecha de inicio
    catchup=False,
    default_args=default_args,
)

# Ejecutar el script local en la máquina anfitriona
etl_task = BashOperator(
    task_id='ejecutar_etl_local_task',
    bash_command="python3 /opt/airflow/etl_scripts/etl_process.py",  # Ruta al script en el contenedor
    dag=dag,
)

etl_task  # Definir la tarea del DAG
