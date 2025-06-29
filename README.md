# Data Integration Platform

Este proyecto es un Trabajo de Fin de Grado que implementa una plataforma de integración de datos utilizando Apache Airflow para orquestar procesos ETL (Extract, Transform, Load) que interactúan con SQL Server y RabbitMQ.

## Descripción

La plataforma está diseñada para realizar las siguientes tareas:
- Procesar datos desde una base de datos SQL Server (AdventureWorks)
- Realizar transformaciones y validaciones de datos
- Gestionar métricas de los procesos ETL
- Integración con RabbitMQ para procesamiento de mensajes
- Sistema de publicación/suscripción para procesamiento en tiempo real

## Sistema de Mensajería

La plataforma implementa un sistema de publicación/suscripción usando RabbitMQ que consta de:

1. **Publicador de Datos (Publisher)**
   - Simula la generación de datos de clientes y productos
   - Envía mensajes a un exchange tipo fanout
   - Asegura la persistencia de los mensajes
   - Ubicación: `scripts/rabbitmq/rabbitmq_publisher.py`

2. **Suscriptores (Subscribers)**:
   
   a. **SQL Subscriber**
   - Realiza carga directa a SQL Server
   - Inserta datos en las tablas DimCustomer y DimProduct
   - Manejo de errores y reintentos
   - Ubicación: `scripts/rabbitmq/rabbitmq_subscriber_sql.py`
   
   b. **Airflow Subscriber**
   - Activa el DAG de ETL para procesamiento
   - Pasa los datos como configuración al DAG
   - Manejo de reconexión automática
   - Ubicación: `scripts/rabbitmq/rabbitmq_subscriber_airflow.py`

## Requisitos Previos

- Docker y Docker Compose
- Python 3.8 o superior
- SQL Server con la base de datos AdventureWorks

## Configuración

1. Clonar el repositorio:
```bash
git clone <repository-url>
cd data-integration-platform
```

2. Crear archivo .env en la raíz del proyecto con las siguientes variables:
```env
SQL_SERVER_HOST=your_sql_server_host
SQL_SERVER_PORT=1433
SQL_SERVER_DATABASE=AdventureWorksDW2019
SQL_SERVER_USERNAME=your_username
SQL_SERVER_PASSWORD=your_password
```

3. Iniciar los servicios con Docker Compose:
```bash
docker-compose up -d
```

El archivo `docker-compose.yaml` está configurado para iniciar y coordinar todos los servicios necesarios:

- **postgres**: Base de datos para los metadatos de Airflow
- **redis**: Broker para el executor de Airflow
- **rabbitmq**: Sistema de mensajería con interfaz de gestión
- **airflow-webserver**: Interfaz web de Airflow
- **airflow-scheduler**: Planificador de tareas
- **airflow-worker**: Ejecutor de tareas, preconfigurado con:
  - Driver ODBC para SQL Server
  - python-dotenv para variables de entorno
- **airflow-triggerer**: Manejo de triggers asíncronos
- **flower** (opcional): Monitorización de Celery

4. Configurar las variables de entorno en el worker:
```bash
# Copiar el archivo .env al contenedor (aunque el volumen ya está montado en docker-compose)
docker cp .env data-integration-platform-airflow-worker-1:/opt/airflow/
```

## Estructura del Proyecto

- `dags/`: Contiene los DAGs de Airflow
  - `etl_process_dag.py`: DAG principal para procesos ETL
  - Otros DAGs para procesamiento de transacciones
- `scripts/`: Contiene los scripts de Python para ETL
  - `etl_process.py`: Script principal de ETL
  - `metrics_connection.py`: Manejo de métricas
  - `sql_server_connection_*.py`: Archivos de conexión
- `config/`: Archivos de configuración
- `logs/`: Logs generados por los procesos
- `plugins/`: Plugins de Airflow

## DAGs Disponibles

1. **ETL Process DAG**
   - Propósito: Proceso ETL principal para procesamiento de datos
   - Trigger: Manual o activado por mensajes de RabbitMQ
   - Archivo: `etl_process_dag.py`
   - Procesa datos de clientes y productos
   - Realiza transformaciones y validaciones
   - Registra métricas del proceso

> **Nota**: El proyecto incluye DAGs adicionales (`procesar_transaccion_*.py`) que sirven como ejemplos didácticos desarrollados durante la investigación del TFG para demostrar cómo interactuar entre el contenedor de Airflow y la máquina local. Estos DAGs ilustran técnicas para escribir archivos y logs desde el contenedor hacia el sistema host, útiles para entender la comunicación entre contenedores y el sistema anfitrión.

## Por Qué Se Realizan Estas Configuraciones

1. **Docker Compose Setup**:
   - Coordina todos los servicios necesarios en una única configuración
   - Gestiona las dependencias y el orden de inicio de los servicios
   - Configura healthchecks para asegurar la disponibilidad de los servicios
   - Mantiene volúmenes persistentes para datos importantes
   - Configura la red interna para comunicación entre servicios

2. **RabbitMQ Setup**:
   - Integrado en docker-compose con la interfaz de gestión
   - Puerto 5672: Para la comunicación AMQP (mensajería)
   - Puerto 15672: Para la interfaz web de administración
   - Volumen persistente para mantener los datos entre reinicios
   - Healthcheck configurado para verificar la conectividad

3. **Airflow Worker Setup**:
   - Preconfigurado con el driver ODBC 17 para SQL Server
   - Instalación automática de python-dotenv
   - Gestión de permisos y usuario airflow
   - Montaje de volúmenes para acceder a scripts y datos
   - Dependencias configuradas con RabbitMQ y otros servicios

4. **Variables de Entorno**:
   - Archivo .env montado en el contenedor vía docker-compose
   - Variables disponibles para todos los servicios que las necesiten
   - Gestión segura de credenciales y configuraciones

## Acceso a las Interfaces

- **Airflow UI**: http://localhost:8080
  - Usuario: airflow
  - Contraseña: airflow

- **RabbitMQ Management**: http://localhost:15672
  - Usuario: guest
  - Contraseña: guest

## Notas Importantes

- La configuración del driver ODBC debe realizarse en el contenedor del worker
- Las variables de entorno se cargan desde el archivo .env en el contenedor
- Verifique la instalación del driver antes de ejecutar los procesos ETL
- Los logs se almacenan en la carpeta `logs/`
- Las métricas se guardan en una base de datos separada
- Los suscriptores de RabbitMQ manejan reconexión automática
- El sistema continúa funcionando incluso si un suscriptor está caído temporalmente
- Este proyecto fue desarrollado como parte de un Trabajo de Fin de Grado, sirviendo como demostración práctica de conceptos de integración de datos, uso de contenedores y mensajería asíncrona

## Troubleshooting

Si encuentra problemas con la conexión a SQL Server:
1. Verificar que el driver ODBC está instalado en el contenedor (usar odbcinst -q -d)
2. Comprobar que el archivo .env está presente en /opt/airflow/ del contenedor
3. Verificar que python-dotenv está instalado en el contenedor
4. Comprobar la accesibilidad del servidor SQL desde el contenedor
5. Revisar los logs de Airflow para mensajes de error específicos

Si encuentra problemas con RabbitMQ:
1. Verificar que RabbitMQ está en ejecución: `docker ps | findstr rabbitmq`
2. Comprobar acceso a la interfaz web: http://localhost:15672
3. Verificar que los suscriptores están ejecutándose y conectados
4. Revisar los logs de los suscriptores para mensajes de error
5. Comprobar la configuración de las colas y exchanges en la interfaz de administración
