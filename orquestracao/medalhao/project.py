"""
Aponta o Dagster para o projeto dbt.

O DbtProject é a ponte entre as duas ferramentas. Ele localiza o
`manifest.json` — o arquivo que o dbt gera com TODO o grafo de dependências
do projeto (quais modelos existem, do que cada um depende, quais testes tem).

É a partir desse manifesto que o Dagster monta os assets sozinho. Você não
redeclara nada: o `ref()` que você escreveu no SQL vira uma aresta no grafo
do Dagster.
"""

import os
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent.parent.parent

# O warehouse fica no PostgreSQL configurado em WAREHOUSE_URL. Carregar o .env aqui
# (antes do DbtProject) garante que o manifest e o Dagster usem a mesma
# conexao, inclusive quando `dagster dev` e iniciado sem `source .env`.
load_dotenv(RAIZ / ".env")


def configurar_dbt_para_warehouse() -> None:
    """Deriva as variaveis do perfil dbt a partir de WAREHOUSE_URL.

    A URL fica apenas no .env; host, usuario e senha nao sao duplicados em
    arquivos versionados. `setdefault` permite sobrescrever qualquer campo em
    ambientes de homologacao/producao com DBT_HOST, DBT_USER etc.
    """
    warehouse_url = os.getenv("WAREHOUSE_URL")
    if not warehouse_url:
        return

    parsed = urlparse(warehouse_url)
    if not (parsed.hostname and parsed.path and parsed.username and parsed.password):
        raise ValueError("WAREHOUSE_URL deve ser uma URL PostgreSQL completa.")

    os.environ.setdefault("DBT_HOST", parsed.hostname)
    os.environ.setdefault("DBT_PORT", str(parsed.port or 5432))
    os.environ.setdefault("DBT_USER", unquote(parsed.username))
    os.environ.setdefault("DBT_PASSWORD", unquote(parsed.password))
    os.environ.setdefault("DBT_DBNAME", parsed.path.lstrip("/"))


configurar_dbt_para_warehouse()

# ⚠️ ISTO PRECISA VIR ANTES DE IMPORTAR dagster_dbt.
#
# O dbt está instalado no venv do projeto (.venv/bin/dbt), não no PATH do
# sistema. Vários pontos do dagster-dbt procuram um `dbt` genérico no PATH —
# inclusive o prepare_if_dev() abaixo, que instancia um DbtCliResource próprio
# e ignora qualquer dbt_executable_path que a gente configure em Definitions.
#
# Colocar o venv na frente do PATH resolve para todos esses pontos de uma vez.
_VENV_BIN = str(RAIZ / ".venv" / "bin")
if _VENV_BIN not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _VENV_BIN + os.pathsep + os.environ.get("PATH", "")

from dagster_dbt import DbtProject  # noqa: E402

dbt_project = DbtProject(
    project_dir=RAIZ / "dbt_medalhao",
    # O profiles.yml mora dentro do próprio projeto dbt (versionado, sem
    # segredo — usa env_var com defaults locais).
    profiles_dir=RAIZ / "dbt_medalhao",
)

# Em desenvolvimento, regenera o manifest.json sempre que os modelos mudam.
# Sem isso, você editaria um .sql e o Dagster continuaria mostrando o grafo
# antigo até você rodar `dbt parse` na mão.
# Em produção o manifesto é gerado no build e esta linha vira no-op.
dbt_project.prepare_if_dev()
