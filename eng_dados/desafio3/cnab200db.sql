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