-- Tabela censo_saude
CREATE TABLE censo_saude (
    ANO INT NOT NULL,
    UF INT NOT NULL,
    Codmun6 INT NOT NULL,
    Codmun7 INT NOT NULL,
    Municipio VARCHAR(255) NOT NULL,
    ESPVIDA DECIMAL(10, 2),
    FECTOT DECIMAL(10, 2),
    MORT1 DECIMAL(10, 2),
    MORT5 DECIMAL(10, 2),
    RAZDEP DECIMAL(10, 2),
    SOBRE40 DECIMAL(10, 2),
    SOBRE60 DECIMAL(10, 2),
    T_ENV DECIMAL(10, 2)
);

-- Tabela coordenadas
CREATE TABLE coordenadas (
    codigo_ibge INT NOT NULL,
    nome VARCHAR(100) NOT NULL,
    latitude FLOAT(8) NOT NULL,
    longitude FLOAT(8) NOT NULL,
    capital BOOLEAN NOT NULL,
    codigo_uf INT NOT NULL,
    siafi_id VARCHAR(4) NOT NULL UNIQUE,
    ddd INT NOT NULL,
    fuso_horario VARCHAR(32) NOT NULL
);
