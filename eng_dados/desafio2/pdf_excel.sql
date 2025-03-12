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