from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
import logging

# Configurações de conexão
MONGO_URI = "mongodb://usuario:senha@host:porta/db"
POSTGRES_CONN = {
    "dbname": "fintech",
    "user": "usuario",
    "password": "senha",
    "host": "localhost",
    "port": "5432",
}

# Função para extrair dados do MongoDB
def extract_from_mongo():
    client = MongoClient(MONGO_URI)
    db = client.get_database()
    collection = db.ccbs  # Nome da coleção
    documents = list(collection.find())
    client.close()
    return documents

# Função para transformar os dados extraídos
def transform_data(documents):
    transformed_data = []
    for doc in documents:
        transformed_data.append((
            doc["_id"],
            doc["clientId"],
            doc["taxId"],
            doc["name"],
            doc["type"],
            doc["birthDate"],
            doc.get("averageMonthlyIncome", 0),
            doc["bankData"]["bankNumber"],
            doc["bankData"]["branchNumber"],
            doc["bankData"]["accountNumber"]
        ))
    return transformed_data

# Função para carregar os dados no PostgreSQL
def load_to_postgres(transformed_data):
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO clientes (id, client_id, tax_id, name, type, birth_date, average_income, bank_number, branch_number, account_number)
        VALUES %s
        ON CONFLICT (id) DO NOTHING;
        """
        execute_values(cursor, insert_query, transformed_data)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Erro ao carregar dados no PostgreSQL: {e}")

# Definição da DAG do Airflow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "mongo_to_postgres_etl",
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id="extract_from_mongo",
    python_callable=extract_from_mongo,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    op_args=[extract_task.output],
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    op_args=[transform_task.output],
    dag=dag,
)

extract_task >> transform_task >> load_task
