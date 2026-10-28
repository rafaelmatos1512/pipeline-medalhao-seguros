"""
Atalho para rodar o dbt e o Dagster sem subir o Airbyte.

Copia as tabelas do schema `origem` para o schema `bronze` do warehouse, no
formato que o Airbyte deixaria: as mesmas colunas, mais `_airbyte_extracted_at`.
As colunas ficam como texto; a silver faz o cast, como faria com a bronze real.

Usa ORIGEM_URL para ler e DW_AIRBYTE_URL para escrever (o usuário que só
escreve na bronze), ambos do .env. Cada execução esvazia e recarrega as tabelas da bronze (sem
recriá-las, porque as views da silver dependem delas).

Uso:
    python ingestao/copiar_para_bronze.py
"""

import os
import sys

import psycopg
from dotenv import load_dotenv

TABELAS = ["corretoras", "propostas", "pendencias"]


def main() -> int:
    load_dotenv()
    origem, destino = os.getenv("ORIGEM_URL"), os.getenv("DW_AIRBYTE_URL")
    if not (origem and destino):
        print("Defina ORIGEM_URL e DW_AIRBYTE_URL no .env (veja o .env.example).", file=sys.stderr)
        return 1

    with psycopg.connect(origem) as src, psycopg.connect(destino) as dst:
        for tabela in TABELAS:
            colunas = [r[0] for r in src.execute(
                """SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'origem' AND table_name = %s
                    ORDER BY ordinal_position""", (tabela,))]
            linhas = src.execute(f"SELECT * FROM origem.{tabela}").fetchall()

            dst.execute(
                f"CREATE TABLE IF NOT EXISTS bronze.{tabela} ("
                + ", ".join(f'"{c}" text' for c in colunas)
                + ", _airbyte_extracted_at timestamptz DEFAULT now())"
            )
            dst.execute(f"TRUNCATE bronze.{tabela}")
            with dst.cursor() as cur:
                cur.executemany(
                    f"INSERT INTO bronze.{tabela} ("
                    + ", ".join(f'"{c}"' for c in colunas)
                    + ") VALUES (" + ", ".join(["%s"] * len(colunas)) + ")",
                    [tuple(None if v is None else str(v) for v in linha) for linha in linhas],
                )
            print(f"bronze.{tabela}: {len(linhas)} linhas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
