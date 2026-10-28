-- =============================================================================
--  Warehouse: as três camadas da arquitetura medalhão
--
--  Roda automaticamente na primeira subida do container pg_warehouse.
-- =============================================================================

-- 🥉 BRONZE — o dado exatamente como chegou da origem.
--    Quem escreve aqui: SOMENTE o Airbyte. Nunca editamos à mão.
--    Regra de ouro: bronze é imutável. Se a limpeza estiver errada,
--    reprocessamos a partir daqui sem precisar bater na origem de novo.
CREATE SCHEMA IF NOT EXISTS bronze;
COMMENT ON SCHEMA bronze IS
  'Camada RAW. Escrita exclusiva do Airbyte. Dado bruto, sem transformacao.';

-- 🥈 SILVER — limpo, tipado, deduplicado, conformado.
--    Quem escreve aqui: dbt.
--    Faz: parse de data, normalizacao de CPF/CNPJ, cast de numeros, dedup.
--    NAO faz: regra de negocio.
CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS
  'Camada limpa e conformada. Construida pelo dbt. Sem regra de negocio.';

-- 🥇 GOLD — pronto para consumo (BI, relatorio, aplicacao).
--    Quem escreve aqui: dbt.
--    Faz: regra de negocio, agregacao, metrica.
--    NAO faz: limpeza (isso ja aconteceu na silver).
CREATE SCHEMA IF NOT EXISTS gold;
COMMENT ON SCHEMA gold IS
  'Camada de consumo. Marts com regra de negocio e agregacoes.';


-- =============================================================================
--  Usuários separados por responsabilidade
--
--  Por que nao usar o superusuario pra tudo? Porque separar permissao é o que
--  garante na marra a regra "so o Airbyte escreve na bronze". Se o dbt tentar
--  escrever na bronze, o banco recusa. A arquitetura deixa de depender de
--  disciplina e passa a ser imposta pelo sistema.
-- =============================================================================

-- Usuário do Airbyte: escreve na bronze, e só.
CREATE USER airbyte_user WITH PASSWORD 'airbyte123';
GRANT USAGE, CREATE ON SCHEMA bronze TO airbyte_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
  GRANT ALL ON TABLES TO airbyte_user;

-- ⚠️ CREATE no BANCO (não só no schema) — obrigatório desde "Destinations V2".
-- O destination-postgres não escreve direto na tabela final: ele primeiro
-- grava o JSON cru em tabelas de um schema próprio, o `airbyte_internal`,
-- e só depois faz o "typing & deduping" para a tabela tipada na bronze.
-- Como ele CRIA esse schema sozinho no primeiro sync, precisa desta permissão.
-- Sem ela o sync falha com um erro de permissão pouco óbvio.
GRANT CREATE ON DATABASE dw_medalhao TO airbyte_user;

-- Usuário do dbt: LÊ a bronze, ESCREVE na silver e gold.
CREATE USER dbt_user WITH PASSWORD 'dbt123';
GRANT USAGE ON SCHEMA bronze TO dbt_user;
GRANT USAGE, CREATE ON SCHEMA silver TO dbt_user;
GRANT USAGE, CREATE ON SCHEMA gold   TO dbt_user;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO dbt_user;
-- Tabelas que o Airbyte criar DEPOIS também precisam ser legíveis pelo dbt:
ALTER DEFAULT PRIVILEGES FOR USER airbyte_user IN SCHEMA bronze
  GRANT SELECT ON TABLES TO dbt_user;

-- Usuário de leitura: para BI, pgAdmin, exploração. Só enxerga a gold.
CREATE USER analista_user WITH PASSWORD 'analista123';
GRANT USAGE ON SCHEMA gold TO analista_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO analista_user;
ALTER DEFAULT PRIVILEGES FOR USER dbt_user IN SCHEMA gold
  GRANT SELECT ON TABLES TO analista_user;
