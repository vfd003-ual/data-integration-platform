import pika
import json
import uuid
import time
import random
from datetime import datetime
from faker import Faker

# Primero, definimos los datos de geografía (como variable global)
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
    # Seleccionar una ubicación aleatoria de nuestros datos reales
    location = random.choice(GEOGRAPHY_DATA)
    
    # Generar dirección específica para la ciudad
    street_types = {
        "Rock Springs": ["Canyon", "Ridge", "Mountain", "Rock", "Spring"],
        "Cheyenne": ["Frontier", "Pioneer", "Capitol", "Prairie", "Buffalo"],
        "Plano": ["Legacy", "Preston", "Parker", "Coit", "Independence"],
        "Jefferson City": ["Capitol", "Missouri", "Madison", "Jefferson", "High"]
    }
    
    # Seleccionar un tipo de calle específico para la ciudad
    street_type = random.choice(street_types[location["City"]])
    address = f"{faker.building_number()} {street_type} {random.choice(['St', 'Ave', 'Rd', 'Dr'])}"

    customer_data = {
        "type": "customer",
        "CustomerAlternateKey": f"AW{faker.random_number(digits=8)}",
        "GeographyKey": location["GeographyKey"],  # Usar GeographyKey real
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
        # Usar dirección real de Estados Unidos
        "AddressLine1": address,
        "AddressLine2": None,
        "City": location["City"],
        "StateProvinceCode": location["StateProvinceCode"],
        "PostalCode": location["PostalCode"],
        # Número de teléfono sin formato específico
        "Phone": faker.numerify('##########'),  # 10 dígitos sin formato
        "DateFirstPurchase": faker.date_between(start_date='-5y').strftime('%Y-%m-%d'),
        "CommuteDistance": faker.random_element(
            elements=('0-1 Miles', '1-2 Miles', '2-5 Miles', '5-10 Miles', '10+ Miles')
        )
    }
    return customer_data

# Define lista de colores en inglés de una palabra
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
    # Seleccionar un color consistente para todas las variantes
    selected_color = faker.random_element(elements=colors)
    model_number = faker.random_number(digits=3)
    size = str(faker.random_int(min=48, max=52))
    
    # Obtener la traducción al español y francés del color
    color_translations = get_color_translations(selected_color)
    
    product_data = {
        "type": "product",
        "ProductAlternateKey": f"BK-{faker.random_letter()}{faker.random_number(digits=2)}B-{faker.random_number(digits=2)}",
        "ProductSubcategoryKey": 2,
        "WeightUnitMeasureCode": "LB ",
        "SizeUnitMeasureCode": "CM ",
        "EnglishProductName": f"Road-{model_number} {selected_color}, {size}",
        "SpanishProductName": f"Carretera-{model_number} {color_translations['es']}, {size}",
        "FrenchProductName": f"Vélo Route-{model_number} {color_translations['fr']}, {size}",
        "StandardCost": round(float(faker.random_number(digits=3)), 4),
        "FinishedGoodsFlag": 1,
        "Color": selected_color,
        "SafetyStockLevel": faker.random_int(min=75, max=100),
        "ReorderPoint": faker.random_int(min=50, max=75),
        "ListPrice": round(float(faker.random_number(digits=3)), 2),
        "Size": size,
        "SizeRange": "48-52 CM",
        "Weight": round(float(faker.random_number(digits=2)), 2),
        "DaysToManufacture": faker.random_int(min=1, max=5),
        "ProductLine": faker.random_element(elements=('R ', 'M ', 'T ')),
        "DealerPrice": round(float(faker.random_number(digits=3)), 3),
        "Class": faker.random_element(elements=('H ', 'M ', 'L ')),
        "Style": faker.random_element(elements=('U ', 'M ', 'W ')),
        "ModelName": f"Road-{model_number}",
        "EnglishDescription": "Entry level adult bike; offers a comfortable ride cross-country or down the block. Quick-release hubs and rims.",
        "FrenchDescription": "Vélo d'adulte d'entrée de gamme ; permet une conduite confortable en ville ou sur les chemins de campagne. Moyeux et rayons à blocage rapide.",
        "ChineseDescription": "入门级成人自行车；确保越野旅行或公路骑乘的舒适。快拆式车毂和轮缘。",
        "ArabicDescription": "إنها دراجة مناسبة للمبتدئين من البالغين؛ فهي توفر قيادة مريحة سواءً على الطرق الوعرة أو في ساحة المدينة. يتميز محورا العجلتين وإطاريهما المعدنيين بسرعة التفكيك.",
        "HebrewDescription": "אופני מבוגרים למתחילים; מציעים רכיבה נוחה \"מחוף לחוף\" או לאורך הרחוב. טבורים וחישורים לשחרור מהיר.",
        "ThaiDescription": "จักรยานระดับเริ่มต้นสำหรับผู้ใหญ่ ให้ความสบายในการขับขี่แม้ในเส้นทางทุรกันดารหรือในเมือง  ดุมและขอบล้อถอดได้สะดวก",
        "GermanDescription": "Ein Erwachsenenrad für Einsteiger; bietet Komfort über Land und in der Stadt. Schnellspann-Naben und Felgen.",
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

    faker = Faker('es_ES')  # Configuramos Faker para español

    # Variable para alternar entre customer y product
    is_customer = True

    while True:
        # Generate product data
        selected_color = faker.random_element(elements=colors)
        translations = get_color_translations(selected_color)
        model_number = faker.random_number(digits=3)
        size_number = faker.random_number(digits=2)
        
        product_data = {
            "type": "product",
            "ProductAlternateKey": f"BK-{faker.random_letter()}{faker.random_number(digits=2)}B-{faker.random_number(digits=2)}",
            "ProductSubcategoryKey": 2,  # Siempre será 2
            "WeightUnitMeasureCode": "LB ",  # Espacio adicional requerido
            "SizeUnitMeasureCode": "CM ",    # Espacio adicional requerido
            "EnglishProductName": f"Road-{model_number} {selected_color}, {size_number}",
            "SpanishProductName": f"Carretera: {model_number}, {translations['es']}, {size_number}",
            "FrenchProductName": f"Vélo de route {model_number} {translations['fr']}, {size_number}",
            "StandardCost": round(float(faker.random_number(digits=3)), 4),
            "FinishedGoodsFlag": 1,
            "Color": selected_color,
            "SafetyStockLevel": faker.random_int(min=75, max=100),
            "ReorderPoint": faker.random_int(min=50, max=75),
            "ListPrice": round(float(faker.random_number(digits=3)), 2),
            "Size": str(faker.random_int(min=48, max=52)),
            "SizeRange": "48-52 CM",
            "Weight": round(float(faker.random_number(digits=2)), 2),
            "DaysToManufacture": faker.random_int(min=1, max=5),
            "ProductLine": faker.random_element(elements=('R ', 'M ', 'T ')),
            "DealerPrice": round(float(faker.random_number(digits=3)), 3),
            "Class": faker.random_element(elements=('H ', 'M ', 'L ')),
            "Style": faker.random_element(elements=('U ', 'M ', 'W ')),
            "ModelName": f"Road-{model_number}",
            "EnglishDescription": "Entry level adult bike; offers a comfortable ride cross-country or down the block. Quick-release hubs and rims.",
            "FrenchDescription": "Vélo d'adulte d'entrée de gamme ; permet une conduite confortable en ville ou sur les chemins de campagne. Moyeux et rayons à blocage rapide.",
            "ChineseDescription": "入门级成人自行车；确保越野旅行或公路骑乘的舒适。快拆式车毂和轮缘。",
            "ArabicDescription": "إنها دراجة مناسبة للمبتدئين من البالغين؛ فهي توفر قيادة مريحة سواءً على الطرق الوعرة أو في ساحة المدينة. يتميز محورا العجلتين وإطاريهما المعدنيين بسرعة التفكيك.",
            "HebrewDescription": "אופני מבוגרים למתחילים; מציעים רכיבה נוחה \"מחוף לחוף\" או לאורך הרחוב. טבורים וחישורים לשחרור מהיר.",
            "ThaiDescription": "จักรยานระดับเริ่มต้นสำหรับผู้ใหญ่ ให้ความสบายในการขับขี่แม้ในเส้นทางทุรกันดารหรือในเมือง  ดุมและขอบล้อถอดได้สะดวก",
            "GermanDescription": "Ein Erwachsenenrad für Einsteiger; bietet Komfort über Land und in der Stadt. Schnellspann-Naben und Felgen.",
            "JapaneseDescription": "エントリー レベルに対応する、クロスカントリーにも街への買い物にも快適な、大人の自転車。ハブおよびリムの取りはずしが容易です。",
            "TurkishDescription": "Başlangıç seviyesinde yetişkin bisikleti, kırda veya sokağınızda konforlu sürüş sunar. Kolay çıkarılan göbekler ve jantlar.",
            # Usar formato SQL Server exacto para la fecha
            "StartDate": "2013-07-02",
            "EndDate": None,
            "Status": "Current"
        }

        # Alternar entre customer y product
        if is_customer:
            message = generate_customer_data(faker)
        else:
            message = product_data
            
        is_customer = not is_customer  # Cambiar para la siguiente iteración

        # Configurar propiedades del mensaje para persistencia
        properties = pika.BasicProperties(
            delivery_mode=2,        # hace el mensaje persistente
            content_type='application/json',
            message_id=str(uuid.uuid4()),  # identificador único
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