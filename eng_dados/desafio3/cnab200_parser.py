import os
import psycopg2
import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field, validator
from sqlalchemy import create_engine, Column, Integer, String, Date, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from kafka import KafkaProducer
from dagster import job, op, Out

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

Base = declarative_base()

# Modelo Pydantic para validação
class CNABDetail(BaseModel):
    banco: str = Field(..., min_length=3, max_length=3)
    nosso_numero: str = Field(..., min_length=10, max_length=10)
    numero_documento: str = Field(..., min_length=10, max_length=10)
    data_vencimento: datetime
    valor_titulo: float
    data_pagamento: datetime
    valor_pago: float
    cpf_cnpj: str = Field(..., min_length=11, max_length=15)
    nome_pagador: str

    @validator("data_vencimento", "data_pagamento", pre=True)
    def parse_date(cls, value):
        return datetime.strptime(value, "%Y%m%d").date()

    @validator("valor_titulo", "valor_pago", pre=True)
    def parse_valor(cls, value):
        return int(value) / 100

# Modelo SQLAlchemy para persistência no PostgreSQL
class Pagamento(Base):
    __tablename__ = "pagamentos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    banco = Column(String(3), nullable=False)
    nosso_numero = Column(String(10), nullable=False)
    numero_documento = Column(String(10), nullable=False)
    data_vencimento = Column(Date, nullable=False)
    valor_titulo = Column(Numeric(10, 2), nullable=False)
    data_pagamento = Column(Date, nullable=False)
    valor_pago = Column(Numeric(10, 2), nullable=False)
    cpf_cnpj = Column(String(15), nullable=False)
    nome_pagador = Column(String(30), nullable=False)

# Conexão ao PostgreSQL
def get_db_session():
    engine = create_engine("postgresql://user:password@localhost:5432/cnab")
    Session = sessionmaker(bind=engine)
    return Session()

# Publicação no Kafka
def publish_to_kafka(topic, message):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(topic, message)
    producer.flush()

@op(out=Out(list))
def process_cnab200():
    try:
        file_path = "cnab200.txt"
        logger.info("Iniciando processamento do arquivo CNAB200.")
        
        with open(file_path, 'r') as file:
            lines = file.readlines()
        
        session = get_db_session()
        registros = []

        for line in lines[1:-1]:  # Ignorar header e trailer
            registro = CNABDetail(
                banco=line[1:4].strip(),
                nosso_numero=line[4:14].strip(),
                numero_documento=line[14:24].strip(),
                data_vencimento=line[24:32].strip(),
                valor_titulo=line[32:42].strip(),
                data_pagamento=line[42:50].strip(),
                valor_pago=line[50:62].strip(),
                cpf_cnpj=line[62:77].strip(),
                nome_pagador=line[77:107].strip()
            )
            
            # Logando cada registro processado
            logger.info(f"Processando pagamento: {registro.numero_documento} - {registro.valor_pago}")
            
            pagamento = Pagamento(**registro.dict())
            session.add(pagamento)

            # Publicação no Kafka
            publish_to_kafka("pagamentos", registro.dict())
            registros.append(registro.dict())

        session.commit()
        session.close()
        logger.info("Processamento concluído com sucesso.")
        return registros

    except Exception as e:
        logger.error(f"Erro no processamento CNAB200: {str(e)}")
        raise e

@job
def processamento_cnab_job():
    process_cnab200()

if __name__ == "__main__":
    processamento_cnab_job.execute_in_process()
