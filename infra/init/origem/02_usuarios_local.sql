-- =============================================================================
--  Usuário de leitura para o Airbyte
--
--  O Airbyte nunca deve ter permissão de escrita na origem. Este usuário só
--  lê. A senha abaixo é de demonstração: troque em qualquer uso fora do seu
--  computador.
-- =============================================================================

CREATE USER airbyte_leitor WITH PASSWORD 'leitor123';

GRANT USAGE  ON SCHEMA origem TO airbyte_leitor;
GRANT SELECT ON ALL TABLES IN SCHEMA origem TO airbyte_leitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA origem GRANT SELECT ON TABLES TO airbyte_leitor;

-- Permissão extra para CDC (leitura do WAL). Exige wal_level=logical, que o
-- docker-compose já configura.
ALTER USER airbyte_leitor WITH REPLICATION;
