# 📌 MongoDB to PostgreSQL ETL with Apache Airflow

## 📝 Overview
This repository contains an **ETL pipeline** that extracts data from **MongoDB**, transforms it into a **relational model**, and loads it into **PostgreSQL**. The process is automated using **Apache Airflow** to run periodically.

---

## 🔍 Problem Statement
Our **Fintech** provides loans to **rural producers** and manages different types of credit securities:
- **CCBs** (Cédulas de Crédito Bancário)
- **CPRs** (Cédula de Produto Rural)
- **Promissory Notes**

The **challenge**:
1. **Migrate CCB data from MongoDB to PostgreSQL**, ensuring no duplicates.
2. **Preserve data integrity** while transitioning from a document-based model to a structured relational model.
3. **Handle payment schedules** (installments) in a structured way.
4. **Ensure resilience to failures and consistency in execution**.

---

## ⚙️ Architecture & Design Choices
### **1️⃣ Why Use PostgreSQL?**
- **Relational data modeling**: Our dataset contains well-defined relationships (clients → contracts → installments).
- **ACID compliance**: Ensures consistency and prevents partial updates.
- **Support for indexing & constraints**: Speeds up queries and ensures data integrity.

### **2️⃣ Why Use Airflow?**
- **Scalability**: Manages complex dependencies between tasks.
- **Monitoring & Logging**: Tracks ETL execution and retries failures automatically.
- **Periodic Execution**: Ensures the pipeline keeps data synchronized.

### **3️⃣ Why Use SQLAlchemy?**
- **ORM abstraction**: Allows defining and managing database schema in Python.
- **Data validation & transaction control**: Avoids SQL injection and ensures robust error handling.

---

## 🔧 Implementation Details
### **1️⃣ MongoDB Schema (Document-Based)**
Each record contains:
- **Client Information** (personal data, address, contacts, bank details)
- **Contract Details** (loan values, interest rates, terms)
- **Installments** (amortization, interest, due dates, balances)

### **2️⃣ PostgreSQL Schema (Relational-Based)**
The data is migrated into three **normalized tables**:
#### 🟢 **clients**
```sql
CREATE TABLE clients (
    id UUID PRIMARY KEY,
    client_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    tax_id VARCHAR(14) UNIQUE NOT NULL,
    birth_date DATE,
    gender VARCHAR(20),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(50),
    bank_account_number INTEGER
);
```
#### 🟢 **contracts**
```sql
CREATE TABLE contracts (
    id UUID PRIMARY KEY,
    client_id UUID NOT NULL,
    contract_type VARCHAR(50),
    value NUMERIC(15,2) NOT NULL,
    interest_rate NUMERIC(5,4) NOT NULL,
    total_installments INTEGER NOT NULL,
    initial_due_date DATE,
    final_due_date DATE,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
);
```
#### 🟢 **installments**
```sql
CREATE TABLE installments (
    id SERIAL PRIMARY KEY,
    contract_id UUID NOT NULL,
    installment_number INTEGER NOT NULL,
    due_date DATE NOT NULL,
    principal NUMERIC(15,2),
    interest NUMERIC(15,2),
    total NUMERIC(15,2),
    FOREIGN KEY (contract_id) REFERENCES contracts(id) ON DELETE CASCADE
);
```
---

## 🔄 ETL Process & Data Flow
### **1️⃣ Extract**
- The data is fetched from **MongoDB** using `pymongo`.
- Query returns all **clients, contracts, and installments**.

### **2️⃣ Transform**
- Data is **mapped** into relational tables.
- Data types are **converted** to match PostgreSQL.
- **Validation & cleaning**: Ensuring completeness and integrity.

### **3️⃣ Load**
- Data is **inserted into PostgreSQL** via `SQLAlchemy` ORM.
- Uses **UPSERT** (`session.merge()`) to prevent duplicates.
- **Transactions ensure atomicity**: If an error occurs, the process is rolled back.

### **4️⃣ Automate Execution**
- **Apache Airflow DAG** triggers the ETL **every hour**.
- **Retries on failure** with logging enabled.

---

## 🚀 How to Run
### **1️⃣ Setup Dependencies**
```sh
pip install pymongo sqlalchemy psycopg2 apache-airflow
```

### **2️⃣ Start PostgreSQL**
```sh
docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
```

### **3️⃣ Start MongoDB**
```sh
docker run --name mongo -p 27017:27017 -d mongo
```

### **4️⃣ Configure Airflow**
```sh
export AIRFLOW_HOME=~/airflow
airflow db init
airflow webserver -p 8080 &
airflow scheduler
```

### **5️⃣ Run ETL Pipeline**
```sh
airflow dags list
airflow dags trigger mongo_to_postgres_etl
```

---

## ⚠️ Challenges & Solutions
### **1️⃣ Handling Duplicates**
✅ Solution: Uses **UPSERT (`merge()`)** in SQLAlchemy to prevent inserting duplicate data.

### **2️⃣ Ensuring Referential Integrity**
✅ Solution: **Chaves estrangeiras (`FOREIGN KEY`)** in PostgreSQL prevent orphan records.

### **3️⃣ Handling Failures & Retries**
✅ Solution: **Airflow retries** failed tasks automatically and logs errors.

### **4️⃣ Optimizing Query Performance**
✅ Solution: **Indexes on foreign keys (`client_id`, `contract_id`)** improve query performance.

---

## 📈 Monitoring & Future Improvements
- **Grafana** for real-time monitoring of ETL execution.
- **Partitioning Installments Table** for better performance on large datasets.
- **Airbyte** as an alternative to manage ETL orchestration.

---

## 🏆 Conclusion
This **ETL pipeline** ensures **seamless migration** from MongoDB to PostgreSQL while preserving data integrity, preventing duplication, and enabling efficient querying. 🚀

Feel free to **contribute** or **report issues**! 😊

