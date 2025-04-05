from airflow import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Definición de constantes
CSV_FILE_PATH = (
    "/opt/airflow/dags/data/home_temperature_and_humidity_smoothed_filled.csv"
)
KAFKA_TOPIC = "sensores"
HDFS_PATH = "/topics/sensores/partition=0"
HIVE_DB = "sensores_db"
HIVE_TABLE = "sensores"


# Función que lee el archivo CSV y genera los mensajes para Kafka
def produce_messages():
    logger.info("Leyendo archivo CSV")
    df = pd.read_csv(CSV_FILE_PATH)
    messages = df.to_dict(orient="records")
    logger.info(f"Se han preparado {len(messages)} mensajes para Kafka")
    return [(f"mensaje_{i}", json.dumps(record)) for i, record in enumerate(messages)]


# Función para crear la base de datos en Hive
def create_database():
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
        cursor.close()
        conn.close()
        logger.info(f"✅ Base de datos '{HIVE_DB}' creada correctamente en Hive")
    except Exception as e:
        logger.error(f"❌ Error al crear la base de datos en Hive: {e}")
        raise


# Función para borrar la tabla en Hive
def drop_table():
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {HIVE_DB}.{HIVE_TABLE}")
        cursor.close()
        conn.close()
        logger.info(f"✅ Tabla {HIVE_DB}.{HIVE_TABLE} borrada correctamente en Hive")
    except Exception as e:
        logger.error(f"❌ Error al borrar la tabla en Hive: {e}")
        raise


# Función para crear la tabla en Hive
def create_table():
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.{HIVE_TABLE} (
                    timestamp_reg TIMESTAMP,
                    temperature_salon FLOAT,
                    humidity_salon FLOAT,
                    air_salon FLOAT,
                    temperature_chambre FLOAT,
                    humidity_chambre FLOAT,
                    air_chambre FLOAT,
                    temperature_bureau FLOAT,
                    humidity_bureau FLOAT,
                    air_bureau FLOAT,
                    temperature_exterieur FLOAT,
                    humidity_exterieur FLOAT,
                    air_exterieur FLOAT
                )
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
                STORED AS TEXTFILE
                LOCATION '{HDFS_PATH}'
            """
        )
        cursor.close()
        conn.close()
        logger.info(f"✅ Tabla {HIVE_DB}.{HIVE_TABLE} creada correctamente en Hive")
    except Exception as e:
        logger.error(f"❌ Error al crear la tabla en Hive: {e}")
        raise


def query_hive(query):
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        logger.info("Resultados de Hive:")
        for row in rows:
            logger.info(row)
    except Exception as e:
        logger.error(f"❌ Error al consultar Hive: {e}")
        raise


# Configuración del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "dag_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Enviar mensajes a Kafka
    produce_kafka_messages = ProduceToTopicOperator(
        task_id="produce_kafka_messages",
        topic=KAFKA_TOPIC,
        kafka_config_id="kafka_default",
        producer_function=produce_messages,
    )

    # Esperar a que los datos estén disponibles en HDFS
    wait_for_hdfs = WebHdfsSensor(
        task_id="check_hdfs",
        webhdfs_conn_id="hdfs_default",
        filepath=HDFS_PATH,
        poke_interval=10,
        timeout=600,
    )

    # Crear la base de datos en Hive
    create_database_task = PythonOperator(
        task_id="create_hive_database", python_callable=create_database
    )

    # Borrar la tabla en Hive
    drop_table_task = PythonOperator(
        task_id="drop_hive_table", python_callable=drop_table
    )

    # Crear la tabla en Hive
    create_table_task = PythonOperator(
        task_id="create_hive_table", python_callable=create_table
    )

    # Ejecutar consultas en Hive
    query_avg_temp = PythonOperator(
        task_id="query_avg_temp",
        python_callable=query_hive,
        op_args=[
            f"""
                SELECT 
                    DATE(timestamp_reg) AS day,
                    AVG(temperature_salon) AS avg_temperature_salon,
                    AVG(temperature_chambre) AS avg_temperature_chambre,
                    AVG(temperature_bureau) AS avg_temperature_bureau,
                    AVG(temperature_exterieur) AS avg_temperature_exterieur
                FROM {HIVE_DB}.{HIVE_TABLE}
                GROUP BY DATE(timestamp_reg)
            """,
        ],
    )

    query_poor_air_quality = PythonOperator(
        task_id="query_poor_air_quality",
        python_callable=query_hive,
        op_args=[
            f"""
                SELECT 
                    timestamp_reg,
                    GREATEST(air_salon, air_chambre, air_bureau, air_exterieur) AS worst_air_quality
                FROM {HIVE_DB}.{HIVE_TABLE}
                ORDER BY worst_air_quality DESC
                LIMIT 10
            """,
        ],
    )

    query_humidity_variations = PythonOperator(
        task_id="query_humidity_variations",
        python_callable=query_hive,
        op_args=[
            f"""
                WITH humidity_with_avg AS (
                    SELECT 
                        timestamp_reg,
                        (humidity_salon + humidity_chambre + humidity_bureau + humidity_exterieur) / 4.0 AS avg_humidity
                    FROM {HIVE_DB}.{HIVE_TABLE}
                ),
                humidity_variation AS (
                    SELECT 
                        timestamp_reg,
                        avg_humidity,
                        LAG(avg_humidity, 4) OVER (ORDER BY timestamp_reg) AS avg_humidity_1h_ago
                    FROM humidity_with_avg
                )
                SELECT 
                    timestamp_reg,
                    avg_humidity,
                    avg_humidity_1h_ago,
                    ABS(avg_humidity - avg_humidity_1h_ago) AS variation
                FROM humidity_variation
                WHERE ABS(avg_humidity - avg_humidity_1h_ago) > 5
                ORDER BY timestamp_reg
            """,
        ],
    )

    # Definir dependencias
    (
        produce_kafka_messages
        >> wait_for_hdfs
        >> create_database_task
        >> drop_table_task
        >> create_table_task
        >> query_avg_temp
        >> query_poor_air_quality
        >> query_humidity_variations
    )
