import pymongo
import psycopg2
from psycopg2.extras import execute_values
import json
from datetime import datetime

# Configurações
MONGO_URI = "mongodb://localhost:27017"
POSTGRES_CONN = "dbname=fintech user=postgres password=secret host=localhost"

# Conexão com MongoDB
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db = mongo_client["fintech"]
mongo_collection = mongo_db["contratos"]

# Conexão com PostgreSQL
pg_conn = psycopg2.connect(POSTGRES_CONN)
pg_cursor = pg_conn.cursor()

def transform_cliente(data):
    """Transforma dados do cliente do MongoDB para o PostgreSQL."""
    return (
        data["_id"], data["clientId"], data["name"], data["taxId"], data["type"], 
        data.get("birthDate"), data.get("maritalStatus"), json.dumps(data.get("address", {})), 
        json.dumps(data.get("contacts", {})), data.get("averageMonthlyIncome"), 
        json.dumps(data.get("bankData", {}))
    )

def extract_transform_load():
    """Extrai os dados do MongoDB, transforma e carrega no PostgreSQL."""
    contratos = list(mongo_collection.find())
    
    for contrato in contratos:
        cliente_data = transform_cliente(contrato)
        
        # Inserir cliente
        pg_cursor.execute("""
            INSERT INTO clientes (mongo_id, client_id, nome, cpf, tipo, nascimento, estado_civil, endereco, contatos, renda_mensal, dados_bancarios)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (mongo_id) DO NOTHING;
        """, cliente_data)
        
        # Recuperar cliente_id
        pg_cursor.execute("SELECT id FROM clientes WHERE mongo_id = %s", (contrato["_id"],))
        cliente_id = pg_cursor.fetchone()[0]
        
        # Inserir contrato
        pg_cursor.execute("""
            INSERT INTO contratos (cliente_id, conta, valor_bruto, valor_liquido, taxa_juros, taxa_juros_anual, cet, cet_anual, data_liberacao, data_vencimento_inicial, data_vencimento_final, numero_parcelas)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (
            cliente_id, contrato["operation"].get("Conta"), contrato["operation"].get("ValorBruto"), 
            contrato["operation"].get("ValorLiquido"), contrato["operation"].get("TaxaDeJuros"), 
            contrato["operation"].get("TaxaDeJurosAnual"), contrato["operation"].get("CET"), 
            contrato["operation"].get("CET_ANUAL"), contrato["operation"].get("DataDeLiberacao"), 
            contrato["operation"].get("DataDeVencimentoInicial"), contrato["operation"].get("DataDeVencimentoFinal"), 
            contrato["operation"].get("NumeroDeParcelas")
        ))
        contrato_id = pg_cursor.fetchone()[0]
        
        # Inserir parcelas
        parcelas = contrato["operation"].get("parcelas", {}).get("PrevisaoDeParcela", [])
        parcelas_data = [
            (
                contrato_id, p["NumeroDaParcela"], p["DataDeVencimento"], 
                p["ValorDaAmortizacao"], p["ValorDoJuros"], p["ValorDaPrestacao"], 
                p["ValorDoSaldoAnterior"], p["ValorDoSaldoAtual"]
            )
            for p in parcelas
        ]
        execute_values(pg_cursor, """
            INSERT INTO parcelas (contrato_id, numero_parcela, data_vencimento, valor_amortizacao, valor_juros, valor_prestacao, saldo_anterior, saldo_atual)
            VALUES %s;
        """, parcelas_data)
    
    pg_conn.commit()

if __name__ == "__main__":
    extract_transform_load()
    pg_cursor.close()
    pg_conn.close()
    mongo_client.close()

import pymongo
import psycopg2
from psycopg2.extras import execute_values
import json
import time
import logging
from datetime import datetime
import re

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configurações
MONGO_URI = "mongodb://localhost:27017"
POSTGRES_CONN = "dbname=fintech user=postgres password=secret host=localhost"

def conectar_mongo():
    """Conecta ao MongoDB com tratamento de erro."""
    try:
        return pymongo.MongoClient(MONGO_URI)
    except Exception as e:
        logging.error(f"Erro ao conectar no MongoDB: {e}")
        return None

def conectar_postgres():
    """Conecta ao PostgreSQL com tratamento de erro."""
    try:
        return psycopg2.connect(POSTGRES_CONN)
    except Exception as e:
        logging.error(f"Erro ao conectar no PostgreSQL: {e}")
        return None

def validar_cpf(cpf):
    """Valida e formata CPF."""
    cpf = re.sub(r'[^0-9]', '', cpf)
    return cpf if len(cpf) == 11 else None

def validar_data(data):
    """Valida e converte data."""
    try:
        return datetime.strptime(data, "%Y-%m-%d").date()
    except ValueError:
        return None

def registrar_erro(pg_cursor, etapa, registro, erro):
    """Registra erro no PostgreSQL para auditoria."""
    pg_cursor.execute("""
        INSERT INTO erros_etl (etapa, registro, erro)
        VALUES (%s, %s, %s);
    """, (etapa, json.dumps(registro), str(erro)))

def executar_com_retentativas(funcao, tentativas=5):
    """Executa função com retry exponencial."""
    delay = 2
    for i in range(tentativas):
        try:
            return funcao()
        except Exception as e:
            logging.warning(f"Erro detectado: {e}, tentativa {i+1} de {tentativas}")
            time.sleep(delay)
            delay *= 2
    logging.error("Falha após múltiplas tentativas.")

def extract_transform_load():
    mongo_client = conectar_mongo()
    if not mongo_client:
        return
    mongo_db = mongo_client["fintech"]
    mongo_collection = mongo_db["contratos"]
    pg_conn = conectar_postgres()
    if not pg_conn:
        return
    pg_cursor = pg_conn.cursor()
    contratos = list(mongo_collection.find())
    
    for contrato in contratos:
        try:
            cpf = validar_cpf(contrato.get("taxId"))
            nascimento = validar_data(contrato.get("birthDate"))
            
            pg_cursor.execute("""
                INSERT INTO clientes (mongo_id, client_id, nome, cpf, tipo, nascimento, estado_civil, endereco, contatos, renda_mensal, dados_bancarios)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (mongo_id) DO NOTHING;
            """, (contrato["_id"], contrato["clientId"], contrato["name"], cpf, contrato["type"], nascimento, contrato.get("maritalStatus"), json.dumps(contrato.get("address", {})), json.dumps(contrato.get("contacts", {})), contrato.get("averageMonthlyIncome"), json.dumps(contrato.get("bankData", {}))))
            pg_conn.commit()
        except Exception as e:
            registrar_erro(pg_cursor, "insercao_cliente", contrato, e)
    pg_cursor.close()
    pg_conn.close()
    mongo_client.close()

if __name__ == "__main__":
    extract_transform_load()
