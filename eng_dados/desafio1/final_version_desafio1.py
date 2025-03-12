from pymongo import MongoClient
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Configuração de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuração do MongoDB
MONGO_URI = "mongodb://localhost:27017"
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["finance"]
mongo_collection = mongo_db["clients"]

# Configuração do PostgreSQL
POSTGRES_URI = "postgresql://user:password@localhost:5432/finance"
engine = create_engine(POSTGRES_URI)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Definição das tabelas no PostgreSQL
class Client(Base):
    __tablename__ = 'clients'
    id = Column(String, primary_key=True)
    client_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    tax_id = Column(String, unique=True, nullable=False)
    type = Column(String)
    birth_date = Column(Date)
    marital_status = Column(String)
    birth_place = Column(String)
    mother_name = Column(String)
    gender = Column(String)
    pep = Column(Boolean)
    structure = Column(String)
    state = Column(String)
    average_monthly_income = Column(Float)
    email = Column(String, unique=True)
    phone = Column(String)
    identity_number = Column(String)
    identity_issuer = Column(String)
    identity_issuer_state = Column(String)
    zip_code = Column(String)
    address_state = Column(String)
    city = Column(String)
    address = Column(String)
    neighbourhood = Column(String)
    address_number = Column(String)
    bank_branch_number = Column(Integer)
    bank_account_number = Column(Integer)
    bank_number = Column(String)

class Contract(Base):
    __tablename__ = 'contracts'
    id = Column(String, primary_key=True)
    client_id = Column(String, ForeignKey('clients.id', ondelete="CASCADE"))
    contract_type = Column(String)
    value = Column(Float)
    interest_rate = Column(Float)
    total_installments = Column(Integer)
    first_due_date = Column(Date)
    grace_period = Column(Integer)
    finance_iof = Column(Boolean)
    account_number = Column(String)
    gross_value = Column(Float)
    insurance_value = Column(Float)
    cad_value = Column(Float)
    iof_value = Column(Float)
    net_value = Column(Float)
    annual_interest_rate = Column(Float)
    cet = Column(Float)
    cet_annual = Column(Float)
    release_date = Column(Date)
    initial_due_date = Column(Date)
    final_due_date = Column(Date)
    total_amortization = Column(Float)
    total_service_fee = Column(Float)
    total_insurance = Column(Float)
    total_correction_fee = Column(Float)
    total_interest = Column(Float)
    total_installment_value = Column(Float)
    interest_percentage = Column(Float)
    tax_percentage = Column(Float)
    fee_percentage = Column(Float)
    service_percentage = Column(Float)

class Installment(Base):
    __tablename__ = 'installments'
    id = Column(Integer, primary_key=True, autoincrement=True)
    contract_id = Column(String, ForeignKey('contracts.id', ondelete="CASCADE"))
    installment_number = Column(Integer)
    due_date = Column(Date)
    principal = Column(Float)
    correction_value = Column(Float)
    interest = Column(Float)
    insurance_value = Column(Float)
    bank_fee = Column(Float)
    installment_value = Column(Float)
    previous_balance = Column(Float)
    capitalized_interest = Column(Float)
    current_balance = Column(Float)

# Criar tabelas no PostgreSQL
Base.metadata.create_all(engine)

def fetch_mongo_data():
    """Busca os dados do MongoDB e converte para dicionários adequados."""
    return list(mongo_collection.find())

def transform_and_load():
    """Transforma os dados e insere no PostgreSQL evitando duplicação."""
    try:
        data = fetch_mongo_data()
        for doc in data:
            client = Client(
                id=doc['_id'],
                client_id=doc['clientId'],
                name=doc['name'],
                tax_id=doc['taxId'],
                type=doc.get('type'),
                birth_date=doc.get('birthDate'),
                marital_status=doc.get('maritalStatus'),
                birth_place=doc.get('birthPlace'),
                mother_name=doc.get('motherName'),
                gender=doc.get('gender'),
                pep=doc.get('pep', False),
                structure=doc.get('structure'),
                state=doc.get('state'),
                average_monthly_income=doc.get('averageMonthlyIncome', 0),
                email=doc['contacts'].get('email'),
                phone=doc['contacts'].get('phone'),
                identity_number=doc['identity'].get('number'),
                identity_issuer=doc['identity'].get('issuer'),
                identity_issuer_state=doc['identity'].get('issuerState'),
                zip_code=doc['address'].get('zipCode'),
                address_state=doc['address'].get('state'),
                city=doc['address'].get('city'),
                address=doc['address'].get('address'),
                neighbourhood=doc['address'].get('neighbourhood'),
                address_number=doc['address'].get('number'),
                bank_branch_number=doc['bankData'].get('branchNumber'),
                bank_account_number=doc['bankData'].get('accountNumber'),
                bank_number=doc['bankData'].get('bankNumber')
            )
            session.merge(client)  # Evita duplicação usando UPSERT
            
            contract = Contract(
                id=doc['_id'],
                client_id=doc['_id'],
                contract_type='CCB',
                value=float(doc['operation']['ValorBruto']),
                interest_rate=float(doc['operation']['TaxaDeJuros']),
                total_installments=int(doc['operation']['NumeroDeParcelas']),
                first_due_date=doc['operation'].get('VencimentoPrimeiraParcela'),
                grace_period=doc['operation'].get('Carencia'),
                finance_iof=doc['operation'].get('FinanIOF', False),
                account_number=doc['operation'].get('Conta'),
                gross_value=float(doc['operation']['ValorBruto']),
                insurance_value=float(doc['operation']['ValorDoSeguro']),
                cad_value=float(doc['operation']['ValorDaCAD']),
                iof_value=float(doc['operation']['ValorDoIOF']),
                net_value=float(doc['operation']['ValorLiquido']),
                annual_interest_rate=float(doc['operation']['TaxaDeJurosAnual']),
                cet=float(doc['operation']['CET']),
                cet_annual=float(doc['operation']['CET_ANUAL']),
                release_date=doc['operation'].get('DataDeLiberacao'),
                initial_due_date=doc['operation'].get('DataDeVencimentoInicial'),
                final_due_date=doc['operation'].get('DataDeVencimentoFinal'),
                total_amortization=float(doc['operation']['TotalDeAmortizacao']),
                total_service_fee=float(doc['operation']['TotalDaTaxaDeServico']),
                total_insurance=float(doc['operation']['TotalDoSeguro']),
                total_correction_fee=float(doc['operation']['TotalDaTaxaDeCorrecao']),
                total_interest=float(doc['operation']['TotalDeJuros']),
                total_installment_value=float(doc['operation']['TotalDoValorDasParcelas']),
                interest_percentage=float(doc['operation']['detalhamentoDaCET']['PorcentagemDeJuros']),
                tax_percentage=float(doc['operation']['detalhamentoDaCET']['PorcentagemDeImpostos']),
                fee_percentage=float(doc['operation']['detalhamentoDaCET']['PorcentagemDeTarifas']),
                service_percentage=float(doc['operation']['detalhamentoDaCET']['PorcentagemDeServicos'])
            )
            session.merge(contract)
            
            for parcel in doc['operation']['parcelas']['PrevisaoDeParcela']:
                installment = Installment(
                    contract_id=doc['_id'],
                    installment_number=int(parcel['NumeroDaParcela']),
                    due_date=parcel['DataDeVencimento'],
                    principal=float(parcel['ValorDaAmortizacao']),
                    correction_value=float(parcel['ValorDaCorrecao']),
                    interest=float(parcel['ValorDoJuros']),
                    insurance_value=float(parcel['ValorDoSeguro']),
                    bank_fee=float(parcel['ValorTaxaBancaria']),
                    installment_value=float(parcel['ValorDaPrestacao']),
                    previous_balance=float(parcel['ValorDoSaldoAnterior']),
                    capitalized_interest=float(parcel['ValorDoJurosCapitalizados']),
                    current_balance=float(parcel['ValorDoSaldoAtual'])
                )
                session.add(installment)
        
        session.commit()
    except Exception as e:
        logging.error(f"Erro durante ETL: {e}")
        session.rollback()

def airflow_task():
    """Task do Airflow para executar a extração, transformação e carga."""
    logging.info("Iniciando ETL via Airflow...")
    transform_and_load()
    logging.info("ETL concluído com sucesso.")

# Configuração do DAG no Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mongo_to_postgres_etl',
    default_args=default_args,
    description='Pipeline ETL de MongoDB para PostgreSQL',
    schedule_interval=timedelta(hours=1),
)

task_run_etl = PythonOperator(
    task_id='run_etl',
    python_callable=airflow_task,
    dag=dag,
)

task_run_etl
