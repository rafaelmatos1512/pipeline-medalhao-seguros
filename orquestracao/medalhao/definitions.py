"""
Definitions — o ponto de entrada do Dagster.

É aqui que assets, recursos, jobs e schedules se juntam num objeto só.
O comando `dagster dev` procura exatamente por isto.
"""

from pathlib import Path

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import DbtCliResource
from dotenv import load_dotenv

from .assets_airbyte import airbyte_sync_origem
from .assets_dbt import dbt_medalhao_assets
from .project import dbt_project

RAIZ = Path(__file__).resolve().parent.parent.parent

# Credenciais (Airbyte, warehouse) vêm do .env da raiz, nunca do código.
load_dotenv(RAIZ / ".env")

# O dbt vive no venv do projeto, não no PATH do sistema.
DBT_EXECUTAVEL = RAIZ / ".venv" / "bin" / "dbt"


# =============================================================================
#  JOB — o pipeline completo
#
#  `AssetSelection.all()` seleciona TUDO: o sync do Airbyte e todos os modelos
#  dbt. Mas repare que não declaramos ordem nenhuma.
#
#  O Dagster deduz sozinho, porque:
#    - o asset do Airbyte produz  bronze/propostas
#    - o dbt declara que stg_propostas depende de source('bronze','propostas')
#    - as AssetKeys coincidem
#
#  Resultado:
#      airbyte_sync ──► bronze.propostas ──► stg_propostas ──┬─► fct_propostas
#                       bronze.corretoras ─► stg_corretoras ─┘        │
#                                                                     ▼
#                                                    mart_inadimplencia_corretora
# =============================================================================
job_pipeline_completo = define_asset_job(
    name="pipeline_medalhao",
    selection=AssetSelection.all(),
    description="Airbyte (origem → bronze) + dbt (bronze → silver → gold + testes)",
)

# Job só das transformações — útil para iterar em SQL sem bater na origem
# a cada tentativa. Em desenvolvimento você usa este 20 vezes por dia.
job_apenas_dbt = define_asset_job(
    name="apenas_transformacoes",
    selection=AssetSelection.assets(dbt_medalhao_assets),
    description="Só dbt: reprocessa silver e gold a partir da bronze existente",
)

# Job só de ingestão Airbyte — útil para forçar atualização da Bronze
job_apenas_airbyte = define_asset_job(
    name="apenas_ingestao_airbyte",
    selection=AssetSelection.assets(airbyte_sync_origem),
    description="Só Airbyte: sincroniza a origem para as tabelas Bronze",
)

# Job só da camada Gold — útil para reprocessar marts e dashboards rapidamente
job_apenas_gold = define_asset_job(
    name="apenas_camada_gold",
    selection=AssetSelection.key_prefixes("gold"),
    description="Só Gold: reconstrói as tabelas analíticas e marts consumidos pelo Metabase",
)


# =============================================================================
#  SCHEDULE
#
#  Lembre: a connection do Airbyte está em modo MANUAL de propósito. Quem
#  agenda é o Dagster. Ter dois agendadores disparando o mesmo sync causa
#  execução concorrente e trabalho duplicado.
# =============================================================================
schedule_diario = ScheduleDefinition(
    name="pipeline_diario",
    job=job_pipeline_completo,
    # 06:00 todo dia. A origem é atualizada de madrugada; assim pegamos o dia
    # já fechado. Cron: minuto hora dia mês dia-da-semana
    cron_schedule="0 6 * * *",
    execution_timezone="America/Sao_Paulo",
    # Nasce DESLIGADO. Ligar é um clique na UI (Automation → toggle).
    # Evita que clonar o repo dispare execução sem ninguém pedir.
    default_status=DefaultScheduleStatus.STOPPED,
)


defs = Definitions(
    assets=[airbyte_sync_origem, dbt_medalhao_assets],
    jobs=[
        job_pipeline_completo,
        job_apenas_dbt,
        job_apenas_airbyte,
        job_apenas_gold,
    ],
    schedules=[schedule_diario],
    resources={
        # O DbtCliResource é quem executa o dbt de fato. Recebe o projeto e
        # cuida de profiles, target e streaming dos logs para a UI.
        #
        # `dbt_executable_path` é obrigatório aqui porque o dbt está dentro do
        # venv do projeto (.venv/bin/dbt) e não no PATH do sistema. Sem isso o
        # Dagster procura um `dbt` global e falha na validação.
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            dbt_executable_path=str(DBT_EXECUTAVEL),
        ),
    },
)
