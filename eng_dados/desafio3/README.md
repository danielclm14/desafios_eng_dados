# CNAB200 - Processamento, Validação e Persistência

## 📌 Descrição do Projeto
Este projeto tem como objetivo processar arquivos **CNAB200** contendo registros de pagamentos. A solução envolve:
- **Parsing do arquivo CNAB200** para extrair e validar os dados.
- **Persistência dos pagamentos** em um banco de dados relacional (**PostgreSQL**).
- **Publicação dos dados** em uma solução centralizada (**Apache Kafka**).
- **Orquestração** com **Dagster** para garantir execução automática.
- **Monitoramento** usando **Prometheus, Grafana e Logging estruturado**.

A implementação foi feita **prioritariamente em Python**, utilizando bibliotecas como **Pydantic, SQLAlchemy, Kafka-python e Dagster**.

---

## 🚀 Tecnologias Utilizadas

| Tecnologia    | Uso no Projeto |
|--------------|---------------|
| Python 3.x   | Linguagem principal |
| PostgreSQL   | Banco de dados relacional |
| Apache Kafka | Mensageria para distribuição dos pagamentos |
| SQLAlchemy   | ORM para interação com PostgreSQL |
| Pydantic     | Validação de dados CNAB200 |
| Kafka-Python | Publicação de mensagens no Kafka |
| Dagster      | Orquestração de pipelines |
| Prometheus   | Monitoramento do sistema |
| Grafana      | Dashboard de métricas |
| Metabase     | Visualização dos pagamentos |
| Docker       | Infraestrutura containerizada |

---

## 📁 Estrutura do Projeto

```
📂 cnab200_project
 ├── 📂 data                # Pasta de arquivos CNAB200
 ├── 📂 dags                # Scripts de pipelines Dagster
 ├── 📂 db                  # Configurações do PostgreSQL
 ├── 📂 kafka               # Configuração do Kafka
 ├── cnab200_parser.py      # Código principal de processamento
 ├── docker-compose.yml     # Configuração dos serviços Docker
 ├── README.md              # Documentação do projeto
 ├── requirements.txt       # Dependências do projeto
```

---

## 📖 Etapas do Desenvolvimento

### **1️⃣ Parsing do CNAB200**
- Utilizamos **slicing de strings** para extrair campos com base nas posições fixas do arquivo.
- O modelo **Pydantic** garante que os valores sejam corretamente convertidos (**datas, valores, CPF/CNPJ**).

### **2️⃣ Validação dos Dados**
- Campos obrigatórios são validados.
- Datas devem ter um formato válido (`YYYYMMDD`).
- Valores financeiros são normalizados para reais (divisão por 100).
- O CPF/CNPJ é tratado para aceitar 11 ou 14 dígitos.

### **3️⃣ Persistência no PostgreSQL**
- Foi criada uma tabela `pagamentos` com os seguintes campos:
  ```sql
  CREATE TABLE pagamentos (
      id SERIAL PRIMARY KEY,
      banco VARCHAR(3) NOT NULL,
      nosso_numero VARCHAR(10) NOT NULL,
      numero_documento VARCHAR(10) NOT NULL,
      data_vencimento DATE NOT NULL,
      valor_titulo NUMERIC(10,2) NOT NULL,
      data_pagamento DATE NOT NULL,
      valor_pago NUMERIC(10,2) NOT NULL,
      cpf_cnpj VARCHAR(15) NOT NULL,
      nome_pagador VARCHAR(30) NOT NULL
  );
  ```
- Utilizamos **SQLAlchemy** para manipulação dos dados de forma segura e eficiente.

### **4️⃣ Publicação no Apache Kafka**
- Os pagamentos são publicados no **tópico `pagamentos`** para garantir escalabilidade.
- **Kafka-Python** é usado para a comunicação entre produtores e consumidores.

### **5️⃣ Orquestração com Dagster**
- Criamos um pipeline no **Dagster** para processar os arquivos diariamente.
- O job `processamento_cnab_job()` executa o processo completo de forma automatizada.

### **6️⃣ Monitoramento e Logging**
- Utilizamos **Prometheus** para coletar métricas do sistema.
- **Grafana** exibe gráficos de processamento e alertas.
- O sistema gera logs estruturados para fácil diagnóstico de erros.

---

## 🛠️ Como Rodar o Projeto

### **1️⃣ Subir os Serviços**
```bash
docker-compose up -d
```

### **2️⃣ Criar o Tópico Kafka**
```bash
docker exec -it kafka kafka-topics.sh --create --topic pagamentos --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

### **3️⃣ Executar o Processamento**
```bash
python cnab200_parser.py
```

### **4️⃣ Monitorar os Logs**
```bash
tail -f cnab.log
```

---

## 📊 Visualização dos Dados
### **Metabase**
1. Acesse **[http://localhost:3000](http://localhost:3000)**.
2. Conecte ao PostgreSQL e selecione a tabela `pagamentos`.
3. Crie um **gráfico de valores pagos por dia**.

### **Grafana**
1. Acesse **[http://localhost:3001](http://localhost:3001)**.
2. Conecte ao Prometheus.
3. Adicione métricas de processamento do CNAB200.

---

## 🚀 Próximos Passos
✅ Criar **alertas no Grafana** caso a quantidade de registros seja menor que o esperado.
✅ Implementar **retries** em falhas de conexão com Kafka ou PostgreSQL.
✅ Melhorar a **resiliência** com filas distribuídas no Kafka.

Caso tenha dúvidas ou sugestões, sinta-se à vontade para abrir uma **issue**! 🎯

