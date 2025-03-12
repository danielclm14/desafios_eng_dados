from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl_mongodb_postgres import extract_transform_load

# Definição do DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_mongo_postgres",
    default_args=default_args,
    description="ETL que migra dados do MongoDB para o PostgreSQL",
    schedule_interval=timedelta(hours=6),  # Executa a cada 6 horas
    catchup=False,
)

etl_task = PythonOperator(
    task_id="execute_etl",
    python_callable=extract_transform_load,
    dag=dag,
)

etl_task


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from etl_mongodb_postgres import extract_transform_load

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 8),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "etl_mongo_postgres",
    default_args=default_args,
    description="ETL que migra dados do MongoDB para o PostgreSQL com robustez",
    schedule_interval=timedelta(hours=6),  # Executa a cada 6 horas
    catchup=False,
)

def executar_etl_com_logs():
    """Executa o ETL com logging estruturado."""
    try:
        logging.info("Iniciando ETL...")
        extract_transform_load()
        logging.info("ETL finalizado com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante a execução do ETL: {e}")
        raise e

etl_task = PythonOperator(
    task_id="execute_etl",
    python_callable=executar_etl_com_logs,
    dag=dag,
)

etl_task
