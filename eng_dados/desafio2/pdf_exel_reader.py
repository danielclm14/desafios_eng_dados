import pdfplumber
import fitz  # PyMuPDF
import re
import pandas as pd
from sqlalchemy import create_engine

# ==========================
# 1. EXTRAÇÃO DE DADOS DE PDFs
# ==========================

def extract_pdf_data(pdf_path):
    """Extrai o texto de um arquivo PDF."""
    data = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                data.append(text)
    return "\n".join(data)

def extract_key_information(text):
    """Extrai informações-chave de um texto usando expressões regulares."""
    patterns = {
        "Número do Contrato": r"Nº DO CONTRATO[:\s]+(\d+)",
        "Tipo do Contrato": r"TIPO DO CONTRATO[:\s]+(\w+)",
        "Valor": r"VALOR[:\s]+([\d,.]+)",
        "Data de Vencimento": r"VENCIMENTO[:\s]+([\d/]+)",
        "Emitente": r"EMITENTE[:\s]+([\w\s]+)",
        "Garantia": r"GARANTIA[:\s]+([\w\s,]+)",
    }
    
    extracted_data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        extracted_data[key] = match.group(1) if match else None
    
    return extracted_data

def process_pdf(pdf_path):
    """Pipeline completo de extração de dados de um PDF."""
    pdf_text = extract_pdf_data(pdf_path)
    extracted_info = extract_key_information(pdf_text)
    return extracted_info

# ==========================
# 2. LEITURA E TRATAMENTO DE PLANILHAS
# ==========================

def load_excel_data(file_path, sheet_name="Planilha1"):
    """Carrega os dados da planilha Excel e define a primeira linha como cabeçalho."""
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    df.columns = df.iloc[0]  # Define a primeira linha como cabeçalho
    df = df[1:].reset_index(drop=True)  # Remove a primeira linha duplicada
    return df

def clean_excel_data(df):
    """Normaliza os dados da planilha, corrigindo datas, valores e colunas."""
    # Normalizar datas
    date_cols = ["DATA DE FORMALIZAÇÃO", "DATA DE DESEMBOLSO", "VENCIMENTO"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    
    # Converter valores numéricos
    df["VALOR"] = df["VALOR"].replace(",", ".", regex=True).astype(float)
    
    # Remover espaços extras dos nomes das colunas
    df.columns = df.columns.str.strip()
    
    return df

# ==========================
# 3. UNIFICAÇÃO, VALIDAÇÃO E TRANSFORMAÇÃO
# ==========================

def unify_data(pdf_data, excel_data):
    """Unifica os dados extraídos dos PDFs e da planilha."""
    unified_df = pd.DataFrame(pdf_data).append(excel_data, ignore_index=True)
    
    # Remover duplicatas
    unified_df.drop_duplicates(subset=["Número do Contrato"], inplace=True)
    
    # Validação de campos essenciais
    mandatory_fields = ["Número do Contrato", "Emitente", "Valor", "Data de Vencimento"]
    for field in mandatory_fields:
        unified_df = unified_df.dropna(subset=[field])
    
    return unified_df

# ==========================
# 4. CARGA PARA O BANCO DE DADOS
# ==========================

def save_to_postgresql(df, db_url, table_name="cpr_notas_promissorias"):
    """Salva os dados no banco de dados PostgreSQL."""
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists="replace", index=False)

# ==========================
# EXECUÇÃO DO PROCESSO
# ==========================

def main(pdf_paths, excel_path, db_url):
    # Processar PDFs
    pdf_data = [process_pdf(pdf) for pdf in pdf_paths]

    # Processar Planilha
    excel_data = load_excel_data(excel_path)
    excel_data_cleaned = clean_excel_data(excel_data)

    # Unificar os dados
    final_df = unify_data(pdf_data, excel_data_cleaned)

    # Salvar no banco de dados
    save_to_postgresql(final_df, db_url)

    print("Processamento concluído e dados armazenados com sucesso.")

# ==========================
# CHAMADA PRINCIPAL
# ==========================

if __name__ == "__main__":
    pdf_files = ["caminho/do/arquivo1.pdf", "caminho/do/arquivo2.pdf"]  # Substituir pelos caminhos reais
    excel_file = "caminho/do/arquivo.xlsx"  # Substituir pelo caminho real
    database_url = "postgresql://usuario:senha@localhost:5432/meubanco"

    main(pdf_files, excel_file, database_url)
