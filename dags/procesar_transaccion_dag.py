from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
import random

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
    'procesar_transaccion_dag',
    default_args=default_args,
    description='Procesa transacciones recibidas de RabbitMQ',
    schedule_interval=None,  # Este DAG solo se ejecuta cuando se activa manualmente
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Primera tarea: Mostrar el primer mensaje
def mostrar_mensaje_1(**context):
    """Muestra el primer mensaje por consola"""
    # Obtener los datos del mensaje desde la configuración del DAG
    mensaje = context['dag_run'].conf
    
    if not mensaje:
        logging.error("No se recibieron datos en la configuración del DAG")
        return False
    
    # Mostrar el primer mensaje por consola
    mensaje_consola = f"MENSAJE 1: Procesando transacción con ID: {mensaje.get('id')} para {mensaje.get('nombre')}"
    print(mensaje_consola)
    logging.info(mensaje_consola)
    
    # Guardar el mensaje para la siguiente tarea
    context['ti'].xcom_push(key='mensaje_datos', value=mensaje)
    
    return True

# Segunda tarea: Mostrar el segundo mensaje
def mostrar_mensaje_2(**context):
    """Muestra el segundo mensaje por consola"""
    # Obtener el mensaje de la tarea anterior
    mensaje = context['ti'].xcom_pull(task_ids='mostrar_mensaje_1', key='mensaje_datos')
    
    if not mensaje:
        logging.error("No se pudo obtener los datos del mensaje")
        return False
    
    # Mostrar el segundo mensaje por consola
    monto = mensaje.get('monto', 0)
    fecha = mensaje.get('fecha', 'fecha desconocida')
    mensaje_consola = f"MENSAJE 2: Transacción completada - Monto: ${monto} - Fecha: {fecha}"
    print(mensaje_consola)
    logging.info(mensaje_consola)
    
    return True

# Definir las tareas
tarea_mensaje_1 = PythonOperator(
    task_id='mostrar_mensaje_1',
    python_callable=mostrar_mensaje_1,
    provide_context=True,
    dag=dag,
)

tarea_mensaje_2 = PythonOperator(
    task_id='mostrar_mensaje_2',
    python_callable=mostrar_mensaje_2,
    provide_context=True,
    dag=dag,
)

# Definir el orden de las tareas
tarea_mensaje_1 >> tarea_mensaje_2