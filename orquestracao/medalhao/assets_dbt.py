"""
Assets do dbt.

As ~15 linhas abaixo geram UM ASSET PARA CADA MODELO do projeto dbt, com as
dependências corretas entre eles e os testes acoplados. Não há lista de
modelos aqui, nem ordem declarada: tudo é lido do manifest.json.

Se você criar `models/gold/mart_funil.sql` amanhã, ele aparece no grafo do
Dagster sozinho. É por isso que a integração Dagster+dbt é considerada a
melhor do mercado — as duas ferramentas concordam sobre o que é a unidade de
trabalho: a TABELA, não a tarefa.
"""

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_medalhao_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Constrói silver e gold.

    Usamos `build` e não `run` de propósito:

        dbt run    → só constrói os modelos
        dbt build  → constrói E roda os testes, modelo a modelo

    Com `build`, se `stg_propostas` falhar num teste, os modelos que dependem
    dele são PULADOS. O dado ruim para na silver em vez de contaminar a gold.
    Essa é a diferença entre um pipeline que quebra e um que mente.
    """
    yield from dbt.cli(["build"], context=context).stream()
