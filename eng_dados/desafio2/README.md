# **Solução para Extração, Padronização e Armazenamento de CPRs e Notas Promissórias**

## **1. Visão Geral**
Este projeto implementa uma solução completa para extrair informações de Cédulas de Produto Rural (CPRs) e Notas Promissórias, armazenadas em arquivos PDF e planilhas Excel. O objetivo é estruturar esses dados e carregá-los em um banco de dados PostgreSQL para análise.

## **2. Tecnologias Utilizadas**
- **Python**: Linguagem principal pela robustez na manipulação de dados.
- **pdfplumber & PyMuPDF (fitz)**: Para extração de textos de arquivos PDF.
- **re (Regex)**: Para capturar padrões textuais relevantes.
- **pandas**: Para tratamento e normalização de dados tabulares.
- **sqlalchemy**: Para conexão e manipulação do banco de dados PostgreSQL.
- **PostgreSQL**: Banco de dados utilizado para armazenamento.

## **3. Estrutura do Projeto**

```
/
|-- main.py  # Arquivo principal que executa todo o pipeline
|-- requirements.txt  # Lista de dependências do projeto
|-- data/
|   |-- pdfs/  # Diretório onde os arquivos PDF serão armazenados
|   |-- spreadsheets/  # Diretório onde as planilhas estarão armazenadas
|-- database/
|   |-- schema.sql  # Esquema do banco de dados PostgreSQL
|-- README.md  # Documentação do projeto
```

## **4. Etapas da Solução**

### **4.1 Extração de Dados de PDFs**
#### **Desafio:**
Os PDFs podem ter diferentes formatações (tabelas ou texto corrido), dificultando a extração padronizada.

#### **Solução:**
- Utiliza-se `pdfplumber` para arquivos com tabelas bem estruturadas.
- `PyMuPDF (fitz)` é usado para extração de textos de documentos desestruturados.
- Expressões regulares (`re`) ajudam a capturar informações importantes como números de contrato, valores e datas.

### **4.2 Leitura e Tratamento de Planilhas**
#### **Desafio:**
- As planilhas podem conter dados mal formatados, com erros tipográficos ou espaços desnecessários.
- Datas e valores podem estar representados de forma inconsistente.

#### **Solução:**
- `pandas` é utilizado para carregar e limpar os dados.
- Colunas são padronizadas removendo espaços e ajustando tipos de dados.
- Datas são convertidas para o formato `YYYY-MM-DD`.
- Valores monetários são normalizados para tipo `float`.

### **4.3 Unificação e Validação dos Dados**
#### **Desafio:**
- Garantir que não haja duplicatas.
- Unificar dados de diferentes fontes mantendo consistência.

#### **Solução:**
- `pandas` é usado para combinar os dados dos PDFs e planilhas.
- Remoção de duplicatas baseada no número do contrato.
- Validação de campos obrigatórios (número do contrato, emitente, valor, data de vencimento).

### **4.4 Carga para o Banco de Dados**
#### **Desafio:**
- Escolher uma estrutura eficiente para armazenamento e consultas.
- Garantir a integridade e escalabilidade da base de dados.

#### **Solução:**
- **PostgreSQL** é escolhido por sua robustez e suporte a tipos avançados.
- `sqlalchemy` é utilizado para manipulação e carga dos dados na base.
- A tabela `cpr_notas_promissorias` é estruturada da seguinte forma:

```sql
CREATE TABLE cpr_notas_promissorias (
    id SERIAL PRIMARY KEY,
    numero_contrato VARCHAR(50) UNIQUE,
    tipo_contrato VARCHAR(20),
    emitente VARCHAR(255),
    valor NUMERIC(15,2),
    data_formalizacao DATE,
    data_vencimento DATE,
    garantia TEXT
);
```

## **5. Possíveis Melhorias**
- Implementação de um sistema de logs para monitorar erros durante a extração e carga.
- Uso de uma API para coleta dinâmica de novos PDFs.
- Automatisar a ingestão de dados via `Airflow` para execução periódica.

## **7. Conclusão**
Essa solução permite a extração eficiente de dados de PDFs e planilhas, garantindo um pipeline estruturado para análise em um banco de dados. O uso de `pandas`, `pdfplumber`, `sqlalchemy` e PostgreSQL proporciona uma abordagem escalável e confiável para manipulação de documentos financeiros.

