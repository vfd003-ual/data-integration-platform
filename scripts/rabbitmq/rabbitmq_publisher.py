import pika
import json
import uuid
import time
import random
import sys
from pathlib import Path
from datetime import datetime
from faker import Faker

parent_dir = str(Path(__file__).resolve().parent.parent)
sys.path.append(parent_dir)

from sql_server_connection_local import get_sql_server_connection

# Variables globales para el seguimiento de CustomerAlternateKey
last_db_customer_number = None
customers_created_since_last_query = 0

def get_next_customer_alternate_key():
    global last_db_customer_number, customers_created_since_last_query
    
    # Si es la primera vez o han pasado muchos mensajes, consultar la BD
    if last_db_customer_number is None or customers_created_since_last_query >= 100:
        try:
            conn = get_sql_server_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(CAST(SUBSTRING(CustomerAlternateKey, 3, 8) AS INT)) FROM [AdventureWorksDW2019].[dbo].[DimCustomer]")
            result = cursor.fetchone()[0]
            last_db_customer_number = result if result is not None else 29483
            customers_created_since_last_query = 0
        except Exception as e:
            print(f"Error obteniendo último CustomerAlternateKey: {e}")
            if last_db_customer_number is None:  # Solo usar valor por defecto si no tenemos ningún valor
                last_db_customer_number = 29483
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()
    
    # Incrementar el contador y retornar el siguiente numero
    customers_created_since_last_query += 1
    return last_db_customer_number + customers_created_since_last_query

# Primero, definimos los datos de geografia (como variable global)
GEOGRAPHY_DATA = [
    {
        "GeographyKey": 655, "City": "Rock Springs", "StateProvinceCode": "WY", 
        "StateProvinceName": "Wyoming", "CountryRegionCode": "US",
        "EnglishCountryRegionName": "United States", "SpanishCountryRegionName": "Estados Unidos",
        "FrenchCountryRegionName": "États-Unis", "PostalCode": "82901",
        "SalesTerritoryKey": 1, "IpAddressLocator": "203.0.113.148"
    },
    {
        "GeographyKey": 654, "City": "Cheyenne", "StateProvinceCode": "WY",
        "StateProvinceName": "Wyoming", "CountryRegionCode": "US",
        "EnglishCountryRegionName": "United States", "SpanishCountryRegionName": "Estados Unidos",
        "FrenchCountryRegionName": "États-Unis", "PostalCode": "82001",
        "SalesTerritoryKey": 1, "IpAddressLocator": "203.0.113.147"
    },
    {
        "GeographyKey": 589, "City": "Plano", "StateProvinceCode": "TX",
        "StateProvinceName": "Texas", "CountryRegionCode": "US",
        "EnglishCountryRegionName": "United States", "SpanishCountryRegionName": "Estados Unidos",
        "FrenchCountryRegionName": "États-Unis", "PostalCode": "75074",
        "SalesTerritoryKey": 4, "IpAddressLocator": "203.0.113.82"
    },
    {
        "GeographyKey": 483, "City": "Jefferson City", "StateProvinceCode": "MO",
        "StateProvinceName": "Missouri", "CountryRegionCode": "US",
        "EnglishCountryRegionName": "United States", "SpanishCountryRegionName": "Estados Unidos",
        "FrenchCountryRegionName": "États-Unis", "PostalCode": "65101",
        "SalesTerritoryKey": 3, "IpAddressLocator": "192.0.2.230"
    }
]

def generate_customer_data(faker):
    location = random.choice(GEOGRAPHY_DATA)
    
    street_types = {
        "Rock Springs": ["Canyon", "Ridge", "Mountain", "Rock", "Spring"],
        "Cheyenne": ["Frontier", "Pioneer", "Capitol", "Prairie", "Buffalo"],
        "Plano": ["Legacy", "Preston", "Parker", "Coit", "Independence"],
        "Jefferson City": ["Capitol", "Missouri", "Madison", "Jefferson", "High"]
    }
    
    street_type = random.choice(street_types[location["City"]])
    address = f"{faker.building_number()} {street_type} {random.choice(['St', 'Ave', 'Rd', 'Dr'])}"

    next_customer_number = get_next_customer_alternate_key()

    customer_data = {
        "type": "customer",
        "CustomerAlternateKey": f"AW{str(next_customer_number).zfill(8)}",
        "GeographyKey": location["GeographyKey"],
        "Title": faker.random_element(elements=('Mr.', 'Mrs.', 'Ms.')),
        "FirstName": faker.first_name(),
        "MiddleName": faker.random_letter().upper(),
        "LastName": faker.last_name(),
        "NameStyle": 0,
        "BirthDate": faker.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
        "MaritalStatus": faker.random_element(elements=('M', 'S')),
        "Suffix": None,
        "Gender": faker.random_element(elements=('M', 'F')),
        "EmailAddress": faker.email(),
        "YearlyIncome": round(faker.random_number(digits=5), 2),
        "TotalChildren": faker.random_int(min=0, max=5),
        "NumberChildrenAtHome": faker.random_int(min=0, max=3),
        "EnglishEducation": faker.random_element(elements=('Bachelors', 'Masters', 'PhD')),
        "SpanishEducation": 'Licenciatura',
        "FrenchEducation": 'Bac + 4',
        "EnglishOccupation": 'Professional',
        "SpanishOccupation": 'Profesional',
        "FrenchOccupation": 'Cadre',
        "HouseOwnerFlag": faker.random_element(elements=(0, 1)),
        "NumberCarsOwned": faker.random_int(min=0, max=5),
        "AddressLine1": address,
        "AddressLine2": None,
        "City": location["City"],
        "StateProvinceCode": location["StateProvinceCode"],
        "PostalCode": location["PostalCode"],
        "Phone": faker.numerify('##########'),  # sin formato
        "DateFirstPurchase": faker.date_between(start_date='-5y').strftime('%Y-%m-%d'),
        "CommuteDistance": faker.random_element(
            elements=('0-1 Miles', '1-2 Miles', '2-5 Miles', '5-10 Miles', '10+ Miles')
        )
    }
    return customer_data

colors = ['Red', 'Blue', 'Green', 'Yellow', 'Black', 'White', 'Silver', 'Purple', 'Orange']

def get_color_translations(color_en):
    translations_es = {
        'Red': 'Rojo',
        'Blue': 'Azul',
        'Green': 'Verde',
        'Yellow': 'Amarillo',
        'Black': 'Negro',
        'White': 'Blanco',
        'Silver': 'Plateado',
        'Purple': 'Morado',
        'Orange': 'Naranja'
    }
    translations_fr = {
        'Red': 'Rouge',
        'Blue': 'Bleu',
        'Green': 'Vert',
        'Yellow': 'Jaune',
        'Black': 'Noir',
        'White': 'Blanc',
        'Silver': 'Argenté',
        'Purple': 'Violet',
        'Orange': 'Orange'
    }
    return {
        'es': translations_es.get(color_en, color_en),
        'fr': translations_fr.get(color_en, color_en)
    }

def generate_product_data(faker):
    selected_color = faker.random_element(elements=colors)
    
    model_number = faker.random_element(elements=[750, 760, 770])
    
    size_ranges = ["42-46", "48-52"]
    size_range = faker.random_element(elements=size_ranges)
    min_size, max_size = map(int, size_range.split("-"))
    size = str(faker.random_int(min=min_size, max=max_size))
    
    # Generar ProductAlternateKey basado en el modelo y caracteristicas
    # Format: BK-R19B-44 donde:
    # BK = Bike
    # R = Road
    # 19/20/21 = Dos dígitos basados en el modelo (750->19, 760->20, 770->21)
    # B = Primera letra del color
    # 44 = Tamanio exacto
    model_prefix = {"750": "19", "760": "20", "770": "21"}
    product_alternate_key = f"BK-R{model_prefix[str(model_number)]}{selected_color[0]}-{size}"
    
    # Obtener traducciones de color
    translations = get_color_translations(selected_color)
    
    # Generar precios realistas
    standard_cost = round(faker.random_int(min=340, max=360) + faker.random.random(), 4)  # ~350
    list_price = round(standard_cost + 120, 2)  # ~470 (diferencia de 120)
    dealer_price = round(standard_cost * 1.15, 3)  # 15% mas que standard_cost
    
    product_data = {
        "type": "product",
        "ProductAlternateKey": product_alternate_key,
        "ProductSubcategoryKey": 2,
        "WeightUnitMeasureCode": "LB ",
        "SizeUnitMeasureCode": "CM ",
        "EnglishProductName": f"Road-{model_number} {selected_color}, {size}",
        "SpanishProductName": f"Carretera-{model_number} {translations['es']}, {size}",
        "FrenchProductName": f"Vélo Route-{model_number} {translations['fr']}, {size}",
        "StandardCost": standard_cost,
        "FinishedGoodsFlag": 1,
        "Color": selected_color,
        "SafetyStockLevel": 100,
        "ReorderPoint": 75,
        "ListPrice": list_price,
        "Size": size,
        "SizeRange": f"{size_range} CM",
        "Weight": round(faker.random_int(min=19, max=21) + faker.random.random(), 2),
        "DaysToManufacture": 4,
        "ProductLine": "R ",
        "DealerPrice": dealer_price,
        "Class": "L ",
        "Style": "U ",
        "ModelName": f"Road-{model_number}",
        "EnglishDescription": "Entry level adult bike; offers a comfortable ride cross-country or down the block. Quick-release hubs and rims.",
        "FrenchDescription": "Vélo d'adulte d'entrée de gamme ; permet une conduite confortable en ville ou sur les chemins de campagne. Moyeux et rayons à blocage rapide.",
        "ChineseDescription": "入门级成人自行车；确保越野旅行或公路骑乘的舒适。快拆式车毂和轮缘。",
        "ArabicDescription": "إنها دراجة مناسبة للمبتدئين من البالغين؛ فهي توفر قيادة مريحة سواءً على الطرق الوعرة أو في ساحة المدينة. يتميز محورا العجلتين وإطاريهما المعدنيين بسرعة التفكيك.",
        "HebrewDescription": "אופני מבוגרים למתחילים; מציעים רכיבה נוחה \"מחוף לחוף\" או לאורך הרחוב. טבורים וחישורים לשחרור מהיר.",
        "ThaiDescription": "จักรยานระดับเริ่มต้นสำหรับผู้ใหญ่ ให้ความสบายในการขับขี่แม้ในเส้นทางทุรกันดารหรือในเมือง  ดุมและขอบล้อถอดได้สะดวก",
        "GermanDescription": "Ein Erwachsenenrad für Einsteiger; bietet Komfort über Land and in der Stadt. Schnellspann-Naben und Felgen.",
        "JapaneseDescription": "エントリー レベルに対応する、クロスカントリーにも街への買い物にも快適な、大人の自転車。ハブおよびリムの取りはずしが容易です。",
        "TurkishDescription": "Başlangıç seviyesinde yetişkin bisikleti, kırda veya sokağınızda konforlu sürüş sunar. Kolay çıkarılan göbekler ve jantlar.",
        "StartDate": "2013-07-02",
        "EndDate": None,
        "Status": "Current"
    }
    return product_data

def publish_messages():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    # Declarar exchange como durable
    exchange_name = 'mensajes_fanout_durable'
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

    faker = Faker('es_ES')  
    # Variable para alternar entre customer y product
    is_customer = True

    while True:
        # Alternar entre customer y product usando las funciones de generacion correspondientes
        if is_customer:
            message = generate_customer_data(faker)
        else:
            message = generate_product_data(faker)
            
        is_customer = not is_customer  # Cambiar para la siguiente iteración
        
        # Configurar propiedades del mensaje para persistencia
        properties = pika.BasicProperties(
            delivery_mode=2,        # hace el mensaje persistente
            content_type='application/json',
            message_id=str(uuid.uuid4()),  # identificador unico
            timestamp=int(time.time())     # timestamp del mensaje
        )
        
        channel.basic_publish(
            exchange=exchange_name,
            routing_key='',
            body=json.dumps(message),
            properties=properties
        )
        print(f"[x] Enviado al exchange '{exchange_name}': {message}")
        time.sleep(5)

if __name__ == "__main__":
    publish_messages()