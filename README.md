# Data Integration Platform

Este proyecto es una plataforma de integración de datos que utiliza Apache Airflow para orquestar procesos ETL (Extract, Transform, Load) que interactúan con SQL Server y RabbitMQ.

## Descripción

La plataforma está diseñada para realizar las siguientes tareas:
- Procesar datos desde una base de datos SQL Server (AdventureWorks)
- Realizar transformaciones y validaciones de datos
- Gestionar métricas de los procesos ETL
- Integración con RabbitMQ para procesamiento de mensajes

## Requisitos Previos

- Docker y Docker Compose
- Python 3.8 o superior
- SQL Server con la base de datos AdventureWorks
- ODBC Driver 17 para SQL Server

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

3. Instalar dependencias de Python:
```bash
pip install python-dotenv pyodbc pandas apache-airflow
```

4. Iniciar RabbitMQ:
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

5. Iniciar los servicios de Airflow:
```bash
docker-compose up -d
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
   - Propósito: Proceso ETL principal
   - Trigger: Manual
   - Archivo: `etl_process_dag.py`

2. **Procesar Transacción DAG**
   - Propósito: Procesamiento de transacciones
   - Archivos: `procesar_transaccion_*.py`

## Acceso a las Interfaces

- **Airflow UI**: http://localhost:8080
  - Usuario: airflow
  - Contraseña: airflow

- **RabbitMQ Management**: http://localhost:15672
  - Usuario: guest
  - Contraseña: guest

## Notas Importantes

- Asegúrese de tener instalado el ODBC Driver 17 para SQL Server
- Los logs se almacenan en la carpeta `logs/`
- Las métricas se guardan en una base de datos separada
- Configure las variables de entorno antes de iniciar los servicios

## Troubleshooting

Si encuentra problemas con la conexión a SQL Server:
1. Verificar que el ODBC Driver 17 está instalado
2. Comprobar las variables de entorno en .env
3. Verificar la accesibilidad del servidor SQL

## Contribuir

1. Fork del repositorio
2. Crear una rama para la característica
3. Commit de los cambios
4. Push a la rama
5. Crear un Pull Request

## Licencia

Este proyecto está bajo la Licencia MIT.
