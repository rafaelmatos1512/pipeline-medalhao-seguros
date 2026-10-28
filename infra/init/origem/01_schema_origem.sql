-- =============================================================================
--  ORIGEM: simula o banco de um sistema legado de seguros (dados sintéticos)
--
--  Este schema é feio de propósito. Datas ficam como TEXT em formatos
--  misturados, CPF vem com pontuação, campo vazio é '' e não NULL. Se a origem
--  já viesse limpa, a camada silver não teria o que fazer.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS origem;
COMMENT ON SCHEMA origem IS 'Sistema de origem simulado (dados sintéticos)';


-- -----------------------------------------------------------------------------
-- Colunas de controle, as mais importantes para o Airbyte:
--
--  1) PRIMARY KEY: o Airbyte usa para deduplicar no modo
--     "Incremental | Append + Deduped". Sem PK você só empilha linhas.
--
--  2) CURSOR FIELD (atualizado_em): o Airbyte guarda o maior valor visto e na
--     próxima execução pede só "WHERE atualizado_em >= ultimo_valor". Sem uma
--     coluna assim, todo sync é full refresh.
-- -----------------------------------------------------------------------------

CREATE TABLE origem.corretoras (
    id              BIGSERIAL PRIMARY KEY,
    cnpj            VARCHAR(20)  NOT NULL,   -- formatado: "12.345.678/0001-90"
    cnpj_limpo      VARCHAR(14)  NOT NULL,   -- só dígitos
    razao_social    VARCHAR(255) NOT NULL,
    tipo            VARCHAR(30)  NOT NULL,   -- 'matriz' ou 'filial'
    criado_em       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    atualizado_em   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_corretoras_cnpj UNIQUE (cnpj_limpo)
);


CREATE TABLE origem.propostas (
    id                      BIGSERIAL PRIMARY KEY,
    atualizado_em           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    criado_em               TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    corretora_cnpj          VARCHAR(20)  NOT NULL,
    corretora_razao_social  VARCHAR(255),

    numero_proposta         VARCHAR(50)  NOT NULL,  -- número guardado como texto
    nome_cliente            VARCHAR(255),           -- sempre MAIÚSCULO
    documento_cliente       VARCHAR(20),            -- com pontuação
    produto                 VARCHAR(255),
    linha_negocio           VARCHAR(100),

    -- As três datas são TEXT e em formatos diferentes entre si:
    --   data_criacao, data_referencia -> dd/mm/yyyy
    --   data_vencimento               -> yyyy-mm-dd
    data_criacao            VARCHAR(20),
    data_referencia         VARCHAR(20),
    data_vencimento         VARCHAR(20),

    status_proposta         VARCHAR(100),
    forma_pagamento         VARCHAR(50),            -- inglês e português misturados
    status_pagamento        VARCHAR(50),
    motivo_pendencia        VARCHAR(255),           -- '' quando não há motivo (não é NULL)
    valor                   NUMERIC(15,2),

    -- Chave natural: o que identifica uma proposta no mundo real. Com ela,
    -- recarregar os mesmos dados faz UPDATE em vez de duplicar.
    CONSTRAINT uq_propostas_natural UNIQUE (corretora_cnpj, numero_proposta)
);

CREATE INDEX idx_propostas_atualizado_em ON origem.propostas (atualizado_em);


-- Fotografia diária de parcelas em aberto. Cada carga tem um snapshot_id.
CREATE TABLE origem.pendencias (
    id                      BIGSERIAL PRIMARY KEY,
    atualizado_em           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    snapshot_id             VARCHAR(30)  NOT NULL,

    corretora_cnpj          VARCHAR(20)  NOT NULL,
    corretora_razao_social  VARCHAR(255),

    numero_certificado      VARCHAR(50),
    numero_proposta         VARCHAR(50),
    documento_cliente       VARCHAR(20),
    nome_cliente            VARCHAR(255),
    linha_negocio           VARCHAR(100),
    produto                 VARCHAR(255),
    competencia             VARCHAR(10),
    data_vencimento         VARCHAR(20),
    valor_parcela           NUMERIC(15,2),
    dias_atraso             INTEGER,
    forma_pagamento         VARCHAR(50),
    status_pagamento        VARCHAR(50),
    email                   VARCHAR(255),
    telefone                VARCHAR(30),

    CONSTRAINT uq_pendencias_natural
        UNIQUE (snapshot_id, corretora_cnpj, numero_certificado, competencia, data_vencimento)
);

CREATE INDEX idx_pendencias_atualizado_em ON origem.pendencias (atualizado_em);


-- -----------------------------------------------------------------------------
-- TRIGGER: mantém atualizado_em correto num UPDATE.
--
-- Sem isso, um UPDATE não mexe no cursor e o Airbyte nunca vê a mudança. É a
-- pegadinha nº 1 do sync incremental: o dado muda na origem e
-- silenciosamente nunca chega no warehouse.
--
-- Usa clock_timestamp() e NÃO now(). No Postgres, now() devolve o início da
-- transação. Numa carga em lote, todas as linhas ganham o mesmo instante, o
-- filtro `>=` do Airbyte traz todas de volta a cada sync, e o incremental vira
-- full refresh sem avisar. clock_timestamp() dá um valor por linha.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION origem.fn_atualiza_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.atualizado_em = clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_propostas_atualizado_em
    BEFORE UPDATE ON origem.propostas
    FOR EACH ROW EXECUTE FUNCTION origem.fn_atualiza_timestamp();

CREATE TRIGGER trg_corretoras_atualizado_em
    BEFORE UPDATE ON origem.corretoras
    FOR EACH ROW EXECUTE FUNCTION origem.fn_atualiza_timestamp();

CREATE TRIGGER trg_pendencias_atualizado_em
    BEFORE UPDATE ON origem.pendencias
    FOR EACH ROW EXECUTE FUNCTION origem.fn_atualiza_timestamp();
