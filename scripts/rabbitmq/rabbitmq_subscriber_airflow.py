import pika
import json
import requests
import os
from datetime import datetime
import random
import time

# Airflow API configuration
AIRFLOW_API_URL = os.environ.get('AIRFLOW_API_URL', 'http://localhost:8080/api/v1')
AIRFLOW_USERNAME = os.environ.get('AIRFLOW_USERNAME', 'airflow')
AIRFLOW_PASSWORD = os.environ.get('AIRFLOW_PASSWORD', 'airflow')
DAG_ID = 'etl_process_dag'

def trigger_dag(dag_id, mensaje):
    """Triggers an Airflow DAG with message data as conf"""
    url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    random_suffix = random.randint(1000, 9999)
    dag_run_id = f"manual_{timestamp}_{random_suffix}"
    
    data = {
        "dag_run_id": dag_run_id,
        "conf": mensaje
    }
    print(f"🆔 Generated DAG run ID: {dag_run_id}")
    
    try:
        response = requests.post(
            url,
            json=data,
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
        )
        if response.status_code in [200, 201]:
            # Cambiado para usar el tipo de mensaje en lugar del ID
            print(f"✅ DAG '{dag_id}' activated successfully for {mensaje['type']}")
            return True
        else:
            print(f"❌ Error activating DAG: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error connecting to Airflow: {str(e)}")
        return False

def callback(ch, method, properties, body):
    mensaje = json.loads(body)
    print(f"📥 Received in Airflow subscriber: {mensaje}")
    
    success = trigger_dag(DAG_ID, mensaje)
    
    if success:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("✅ Message confirmed (ACK)")
    else:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        print("⚠️ Message rejected and requeued for retry")

def start_subscriber():
    retry_delay = 5 
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='localhost',
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            channel = connection.channel()

            exchange_name = 'mensajes_fanout_durable'
            channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

            # Crear cola duradera con nombre especifico
            queue_name = 'airflow_subscriber_queue_durable'
            
            # Intentar declarar la cola sin argumentos primero
            try:
                result = channel.queue_declare(queue=queue_name, durable=True)
            except pika.exceptions.ChannelClosedByBroker:
                # Si falla, reconectar y declarar con los argumentos
                connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
                channel = connection.channel()
                result = channel.queue_declare(
                    queue=queue_name, 
                    durable=True
                )
            
            channel.queue_bind(
                exchange=exchange_name,
                queue=queue_name
            )
            
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
            print(f"👂 Subscriber listening on exchange '{exchange_name}' and queue '{queue_name}'")
            print(f"📊 Messages in queue: {result.method.message_count}")
            
            channel.start_consuming()
            
        except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPChannelError) as e:
            print(f"🔄 Connection error: {str(e)}, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            continue
        # ...rest of the error handling...
        except KeyboardInterrupt:
            if channel:
                channel.close()
            if connection:
                connection.close()
            print("🛑 Stopping subscriber...")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            if channel:
                channel.close()
            if connection:
                connection.close()
            time.sleep(retry_delay)
            continue

if __name__ == "__main__":
    start_subscriber()