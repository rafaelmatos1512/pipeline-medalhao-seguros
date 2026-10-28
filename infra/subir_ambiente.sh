#!/usr/bin/env bash
# =============================================================================
#  Sobe todo o ambiente do projeto e só devolve o controle quando estiver
#  REALMENTE pronto para uso.
#
#  Uso:  ./infra/subir_ambiente.sh
#
#  Por que um script e não "docker compose up"?
#  Porque o ambiente tem duas naturezas diferentes:
#
#   1. pg_origem / pg_warehouse / pgadmin  -> containers Docker comuns,
#      sobem com docker compose e voltam sozinhos (restart: unless-stopped).
#
#   2. Airbyte -> UM container Docker (airbyte-abctl-control-plane) que roda
#      um Kubernetes inteiro dentro. Foi criado pelo kind com restart
#      on-failure, então NÃO volta sozinho quando o Docker Desktop reinicia.
#      E mesmo depois de subir, leva 1-3 min até os 8 pods ficarem prontos.
# =============================================================================

set -uo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KIND_CONTAINER="airbyte-abctl-control-plane"
export KUBECONFIG="${HOME}/.airbyte/abctl/abctl.kubeconfig"

echo "═══════════════════════════════════════════════════════"
echo "  Subindo ambiente do projeto medalhão"
echo "═══════════════════════════════════════════════════════"

# ---------------------------------------------------------------------------
# 1. Docker daemon
# ---------------------------------------------------------------------------
if ! docker info >/dev/null 2>&1; then
    echo "→ Docker parado. Abrindo Docker Desktop..."
    open -a Docker
    for i in $(seq 1 60); do
        docker info >/dev/null 2>&1 && break
        sleep 3
    done
    docker info >/dev/null 2>&1 || { echo "✗ Docker não subiu. Abra o Docker Desktop manualmente."; exit 1; }
fi
echo "✓ Docker no ar"

# ---------------------------------------------------------------------------
# 2. Postgres (origem local + warehouse) e pgAdmin
# ---------------------------------------------------------------------------
echo "→ Subindo Postgres e pgAdmin..."
docker compose -f "${RAIZ}/infra/docker-compose.yml" up -d >/dev/null 2>&1
docker exec pg_warehouse pg_isready -U warehouse -d dw_medalhao >/dev/null 2>&1 \
    && echo "✓ pg_warehouse pronto (localhost:5434)" \
    || echo "⚠ pg_warehouse ainda inicializando"

# ---------------------------------------------------------------------------
# 3. Airbyte — o container do cluster kind
# ---------------------------------------------------------------------------
if [ "$(docker inspect -f '{{.State.Running}}' "$KIND_CONTAINER" 2>/dev/null)" != "true" ]; then
    echo "→ Iniciando cluster do Airbyte (pode levar alguns minutos)..."
    docker start "$KIND_CONTAINER" >/dev/null 2>&1 \
        || { echo "✗ Container '$KIND_CONTAINER' não existe. Rode: abctl local install"; exit 1; }
else
    echo "✓ Cluster do Airbyte já estava rodando"
fi

# Esperar a UI responder de verdade. O container subir NÃO significa que o
# Airbyte está pronto — os pods precisam reiniciar dentro do cluster.
echo -n "→ Aguardando a UI responder"
PRONTO=0
for i in $(seq 1 60); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:8000 2>/dev/null)" = "200" ]; then
        PRONTO=1; break
    fi
    echo -n "."
    sleep 5
done
echo

if [ "$PRONTO" = "1" ]; then
    echo "✓ Airbyte pronto (http://localhost:8000)"
else
    echo "⚠ Airbyte ainda não respondeu após 5 min. Diagnostique com:"
    echo "    kubectl get pods -n airbyte-abctl"
fi

# ---------------------------------------------------------------------------
echo
echo "═══════════════════════════════════════════════════════"
echo "  Airbyte     http://localhost:8000   (abctl local credentials)"
echo "  pgAdmin     http://localhost:5050"
echo "  Warehouse   localhost:5434  bronze/silver/gold"
echo "  Origem      localhost:5433  schema origem (dados sintéticos)"
echo "═══════════════════════════════════════════════════════"
