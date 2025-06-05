import sys
from pathlib import Path
import time

parent_dir = str(Path(__file__).resolve().parent.parent)
sys.path.append(parent_dir)


import pika
import json
from datetime import datetime
from sql_server_connection_local import get_sql_server_connection

def get_last_key(cursor, table_name, key_column):
    try:
        cursor.execute(f"SELECT MAX({key_column}) FROM [AdventureWorksDW2019].[dbo].[{table_name}]")
        result = cursor.fetchone()[0]
        return result if result is not None else 0
    except Exception as e:
        print(f"❌ Error obteniendo última clave de {table_name}: {e}")
        return 0

def callback(ch, method, properties, body):
    mensaje = json.loads(body)
    print(f"📥 Recibido mensaje: {mensaje}")

    try:
        conn = get_sql_server_connection()
        if conn:
            cursor = conn.cursor()
            try:
                if mensaje.get('type') == 'customer':
                    # Obtener el último CustomerKey
                    last_customer_key = get_last_key(cursor, 'DimCustomer', 'CustomerKey')
                    next_customer_key = last_customer_key + 1
                    
                    cursor.execute("""
                        SET IDENTITY_INSERT [AdventureWorksDW2019].[dbo].[DimCustomer] ON;
                        
                        INSERT INTO [AdventureWorksDW2019].[dbo].[DimCustomer]
                        (CustomerKey, CustomerAlternateKey, GeographyKey, Title, FirstName, MiddleName, 
                        LastName, NameStyle, BirthDate, MaritalStatus, Suffix, Gender, 
                        EmailAddress, YearlyIncome, TotalChildren, NumberChildrenAtHome, 
                        EnglishEducation, SpanishEducation, FrenchEducation, EnglishOccupation, 
                        SpanishOccupation, FrenchOccupation, HouseOwnerFlag, NumberCarsOwned, 
                        AddressLine1, AddressLine2, Phone, DateFirstPurchase, CommuteDistance)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        
                        SET IDENTITY_INSERT [AdventureWorksDW2019].[dbo].[DimCustomer] OFF;
                    """, (
                        next_customer_key,
                        mensaje['CustomerAlternateKey'],
                        mensaje['GeographyKey'],
                        mensaje['Title'],
                        mensaje['FirstName'],
                        mensaje['MiddleName'],
                        mensaje['LastName'],
                        mensaje['NameStyle'],
                        mensaje['BirthDate'],
                        mensaje['MaritalStatus'],
                        mensaje['Suffix'],
                        mensaje['Gender'],
                        mensaje['EmailAddress'],
                        mensaje['YearlyIncome'],
                        mensaje['TotalChildren'],
                        mensaje['NumberChildrenAtHome'],
                        mensaje['EnglishEducation'],
                        mensaje['SpanishEducation'],
                        mensaje['FrenchEducation'],
                        mensaje['EnglishOccupation'],
                        mensaje['SpanishOccupation'],
                        mensaje['FrenchOccupation'],
                        mensaje['HouseOwnerFlag'],
                        mensaje['NumberCarsOwned'],
                        mensaje['AddressLine1'],
                        mensaje['AddressLine2'],
                        mensaje['Phone'],
                        mensaje['DateFirstPurchase'],
                        mensaje['CommuteDistance']
                    ))
                    print(f"✅ Cliente guardado en DimCustomer con CustomerKey: {next_customer_key}")

                elif mensaje.get('type') == 'product':
                    # Obtener el ultimo ProductKey
                    last_product_key = get_last_key(cursor, 'DimProduct', 'ProductKey')
                    next_product_key = last_product_key + 1
                    
                    cursor.execute("""
                        SET IDENTITY_INSERT [AdventureWorksDW2019].[dbo].[DimProduct] ON;
                        
                        INSERT INTO [AdventureWorksDW2019].[dbo].[DimProduct]
                        (ProductKey, ProductAlternateKey, ProductSubcategoryKey, WeightUnitMeasureCode, 
                        SizeUnitMeasureCode, EnglishProductName, SpanishProductName, FrenchProductName,
                        StandardCost, FinishedGoodsFlag, Color, SafetyStockLevel, ReorderPoint,
                        ListPrice, Size, SizeRange, Weight, DaysToManufacture, ProductLine,
                        DealerPrice, Class, Style, ModelName, LargePhoto, EnglishDescription,
                        FrenchDescription, ChineseDescription, ArabicDescription, HebrewDescription,
                        ThaiDescription, GermanDescription, JapaneseDescription, TurkishDescription,
                        StartDate, EndDate, Status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        
                        SET IDENTITY_INSERT [AdventureWorksDW2019].[dbo].[DimProduct] OFF;
                    """, (
                        next_product_key,
                        mensaje['ProductAlternateKey'],
                        mensaje['ProductSubcategoryKey'],
                        mensaje['WeightUnitMeasureCode'],
                        mensaje['SizeUnitMeasureCode'],
                        mensaje['EnglishProductName'],
                        mensaje['SpanishProductName'],
                        mensaje['FrenchProductName'],
                        mensaje['StandardCost'],
                        mensaje['FinishedGoodsFlag'],
                        mensaje['Color'],
                        mensaje['SafetyStockLevel'],
                        mensaje['ReorderPoint'],
                        mensaje['ListPrice'],
                        mensaje['Size'],
                        mensaje['SizeRange'],
                        mensaje['Weight'],
                        mensaje['DaysToManufacture'],
                        mensaje['ProductLine'],
                        mensaje['DealerPrice'],
                        mensaje['Class'],
                        mensaje['Style'],
                        mensaje['ModelName'],
                        None,  # LargePhoto
                        mensaje.get('EnglishDescription', 'Entry level bike'),
                        mensaje.get('FrenchDescription', 'Vélo d\'entrée de gamme'),
                        mensaje.get('ChineseDescription', '入门级自行车'),
                        mensaje.get('ArabicDescription', 'دراجة مستوى المبتدئين'),
                        mensaje.get('HebrewDescription', 'אופני רמת כניסה'),
                        mensaje.get('ThaiDescription', 'จักรยานระดับเริ่มต้นสำหรับผู้ใหญ่ ให้ความสบายในการขับขี่แม้ในเส้นทางทุรกันดารหรือในเมือง  ดุมและขอบล้อถอดได้สะดวก'),
                        mensaje.get('GermanDescription', 'Einsteiger-Fahrrad'),
                        mensaje.get('JapaneseDescription', 'エントリー レベルに対応する、クロスカントリーにも街への買い物にも快適な、大人の自転車。ハブおよびリムの取りはずしが容易です。'),
                        mensaje.get('TurkishDescription', 'Başlangıç seviyesinde yetişkin bisikleti, kırda veya sokağınızda konforlu sürüş sunar. Kolay çıkarılan göbekler ve jantlar.'),
                        mensaje['StartDate'],
                        mensaje['EndDate'],
                        mensaje['Status']
                    ))
                    print(f"✅ Producto guardado en DimProduct con ProductKey: {next_product_key}")

                conn.commit()
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f"❌ Error insertando en DB: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            finally:
                cursor.close()
                conn.close()
    except Exception as e:
        print(f"❌ Error procesando mensaje: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_subscriber():
    retry_delay = 5  # segundos entre intentos de reconexion
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
            queue_name = 'sql_subscriber_queue_durable'
            
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
            
            # Incluir el binding entre la cola y el exchange
            channel.queue_bind(
                exchange=exchange_name,
                queue=queue_name
            )
            
            # Configurar QoS mas conservador
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
            print(f"👂 Subscriber listening on exchange '{exchange_name}' and queue '{queue_name}'")
            print(f"📊 Messages in queue: {result.method.message_count}")
            
            channel.start_consuming()
            
        except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPChannelError) as e:
            print(f"🔄 Connection error: {str(e)}, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            continue
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
