"""
Asset de ingestão: dispara o sync do Airbyte e espera terminar.

── POR QUE NÃO USAR A BIBLIOTECA `dagster-airbyte`? ───────────────────────────
Existe uma integração oficial, mas ela foi feita para o modelo de autenticação
do Airbyte Cloud e das versões antigas do OSS. O Airbyte 2.x instalado via
abctl usa OAuth client-credentials, e a compatibilidade oscila entre versões.

Falar direto com a API REST são ~40 linhas, funciona de forma previsível e —
mais importante para aprender — deixa visível o que uma integração faz por
baixo: pega token, dispara job, faz polling do status, reporta métricas.

── COMO ESTE ASSET SE CONECTA AO DBT ─────────────────────────────────────────
As AssetKeys abaixo (`bronze/propostas`, `bronze/corretoras`,
`bronze/pendencias`) são EXATAMENTE
as mesmas que o dagster-dbt gera para as `sources` declaradas no
`_bronze_sources.yml`. Com as chaves iguais, o Dagster costura os dois grafos
sozinho:

    airbyte_sync ──► bronze.propostas ──► stg_propostas ──► fct_propostas
                                                        └─► mart_...
                 └─► bronze.pendencias ──► stg_pendencias ─► fct_pendencias

Ninguém declarou essa ligação. Ela emerge das chaves coincidirem.
"""

import os
import time
from pathlib import Path

import requests
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    MaterializeResult,
    multi_asset,
)
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent.parent.parent
load_dotenv(RAIZ / ".env")

# Quanto tempo esperar o sync antes de desistir. Um sync que passa disso
# provavelmente está travado (foi o que aconteceu com o pod em Pending por
# falta de CPU) e é melhor falhar alto do que segurar o pipeline inteiro.
TIMEOUT_SEGUNDOS = 30 * 60
INTERVALO_POLLING = 15


def _get_airbyte_config():
    """Lê as configurações do Airbyte do ambiente."""
    airbyte_url = os.getenv("AIRBYTE_URL", "http://localhost:8000")
    connection_id = os.getenv("AIRBYTE_CONNECTION_ID", "")
    client_id = os.getenv("AIRBYTE_CLIENT_ID", "")
    client_secret = os.getenv("AIRBYTE_CLIENT_SECRET", "")
    return airbyte_url, connection_id, client_id, client_secret


def _token() -> str:
    """Troca client_id/secret por um access token de curta duração."""
    airbyte_url, _, client_id, client_secret = _get_airbyte_config()
    resp = requests.post(
        f"{airbyte_url}/api/public/v1/applications/token",
        json={"client_id": client_id, "client_secret": client_secret},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


@multi_asset(
    specs=[
        AssetSpec(
            key=AssetKey(["bronze", "propostas"]),
            description=(
                "Propostas cruas do sistema de origem, materializadas fisicamente em "
                "bronze.propostas pelo Airbyte."
            ),
            kinds={"airbyte", "postgres"},
        ),
        AssetSpec(
            key=AssetKey(["bronze", "corretoras"]),
            description=(
                "Cadastro de CNPJs consultados, materializado fisicamente em "
                "bronze.corretoras pelo Airbyte."
            ),
            kinds={"airbyte", "postgres"},
        ),
        AssetSpec(
            key=AssetKey(["bronze", "pendencias"]),
            description=(
                "Fotografia diária de parcelas pendentes do sistema de origem, "
                "materializada em bronze.pendencias pelo Airbyte."
            ),
            kinds={"airbyte", "postgres"},
        ),
    ],
    name="airbyte_sync_origem",
    can_subset=True,
    # Sem `compute_kind` aqui: o Dagster recusa quando os AssetSpecs já
    # declaram `kinds`. As duas coisas fazem o mesmo (o ícone na UI), e a
    # versão nos specs é mais específica — cada asset pode ter o seu.
)
def airbyte_sync_origem(context: AssetExecutionContext):
    """Dispara o sync e faz polling até concluir."""
    airbyte_url, connection_id, client_id, client_secret = _get_airbyte_config()

    if not (connection_id and client_id and client_secret):
        raise ValueError(
            "Faltam variáveis no .env: AIRBYTE_CONNECTION_ID, "
            "AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET. "
            "Pegue as credenciais com: abctl local credentials"
        )

    headers = {"Authorization": f"Bearer {_token()}"}

    # --- dispara ---
    resp = requests.post(
        f"{airbyte_url}/api/v1/connections/sync",
        headers=headers,
        json={"connectionId": connection_id},
        timeout=60,
    )
    resp.raise_for_status()
    job_id = resp.json()["job"]["id"]
    context.log.info(f"Sync disparado — job {job_id}")

    # --- espera ---
    inicio = time.time()
    resposta = {}
    while time.time() - inicio < TIMEOUT_SEGUNDOS:
        time.sleep(INTERVALO_POLLING)

        # O token expira; renovar a cada volta é mais barato que tratar 401.
        headers = {"Authorization": f"Bearer {_token()}"}
        info = requests.post(
            f"{airbyte_url}/api/v1/jobs/get",
            headers=headers,
            json={"id": job_id},
            timeout=60,
        )
        info.raise_for_status()
        resposta = info.json()
        status = resposta.get("job", {}).get("status")
        context.log.info(f"job {job_id}: {status}")

        # Airbyte também usa ``incomplete`` quando a tentativa termina antes
        # de completar. É terminal e deve falhar imediatamente no Dagster.
        if status in ("succeeded", "failed", "cancelled", "incomplete", "error"):
            break
    else:
        raise TimeoutError(
            f"Sync {job_id} passou de {TIMEOUT_SEGUNDOS}s sem concluir. "
            "Verifique se o pod ficou em Pending: "
            "kubectl get pods -n airbyte-abctl"
        )

    if resposta.get("job", {}).get("status") != "succeeded":
        raise RuntimeError(
            f"Sync {job_id} terminou como '{resposta.get('job', {}).get('status')}'"
        )

    # --- métricas ---
    # ⚠️ As estatísticas ficam em attempts[].attempt, NÃO no objeto `job`.
    # Ler `job['recordsSynced']` devolve None e vira 0 — um falso "sync
    # vazio" que assusta à toa. Aconteceu aqui.
    tentativa = (resposta.get("attempts") or [{}])[0].get("attempt", {})
    registros = tentativa.get("recordsSynced") or 0
    bytes_sync = tentativa.get("bytesSynced") or 0

    # Contagem por stream, para saber QUAL tabela trouxe o quê.
    por_stream = {
        s.get("streamName"): (s.get("stats") or {}).get("recordsEmitted", 0)
        for s in (tentativa.get("streamStats") or [])
    }

    context.log.info(
        f"Sync concluído: {registros} registros, {bytes_sync} bytes — {por_stream}"
    )

    # Em modo incremental, zero é o resultado ESPERADO quando nada mudou na
    # origem. O que merece suspeita é o contrário: trazer sempre o total
    # completo, sinal de cursor travado (ver a nota sobre clock_timestamp()
    # no schema da origem).
    if registros == 0:
        context.log.info(
            "Zero registros: nada mudou na origem desde o último sync. "
            "É o comportamento correto do incremental."
        )

    for chave, nome in (
        (["bronze", "propostas"], "propostas"),
        (["bronze", "corretoras"], "corretoras"),
        (["bronze", "pendencias"], "pendencias"),
    ):
        asset_key = AssetKey(chave)
        if asset_key in context.selected_asset_keys:
            yield MaterializeResult(
                asset_key=asset_key,
                metadata={
                    "job_id": job_id,
                    "registros_do_stream": por_stream.get(nome, 0),
                    "registros_no_sync": registros,
                    "bytes": bytes_sync,
                },
            )
