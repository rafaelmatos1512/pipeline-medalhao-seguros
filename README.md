# pipeline-medalhao-seguros

Projeto de estudo de engenharia de dados com a stack Airbyte, dbt e Dagster, organizado na arquitetura medalhão (bronze, silver e gold) em cima de um Postgres. Todos os dados são fictícios e gerados por um script, então dá para rodar tudo no seu computador sem depender de nenhum sistema externo.

O cenário é um sistema legado de seguros (propostas, corretoras e parcelas em aberto) que precisa virar relatórios de cobrança e inadimplência. A origem é suja de propósito: datas como texto em formatos diferentes, CPF com pontuação, vazio guardado como string vazia. Se a fonte já viesse limpa, a camada silver não teria o que fazer.

## Como as peças se encaixam

```
  ORIGEM (Postgres)            AIRBYTE              WAREHOUSE (Postgres)
  schema origem      ---- extrai e carrega --->   bronze  (cru, imutável)
  dados sintéticos                                   |
                                                     |  dbt
                                                     v
                                                  silver  (limpo, tipado, sem duplicatas)
                                                     |
                                                     |  dbt
                                                     v
                                                  gold    (fatos, dimensões e marts)

                                   Dagster orquestra o sync do Airbyte e o build do dbt
```

Cada camada tem uma responsabilidade só.

- bronze: guarda o dado como chegou. Só o Airbyte escreve aqui.
- silver: limpa, tipa e remove duplicatas. Não tem regra de negócio.
- gold: regra de negócio e métricas, como situação da proposta, faixa de atraso e taxa de inadimplência.

## O que dá para aprender olhando o código

- Sync incremental do Airbyte e o campo cursor (`atualizado_em`). O arquivo `infra/init/origem/01_schema_origem.sql` explica, nos comentários, por que o trigger usa `clock_timestamp()` e não `now()`. Com `now()`, uma carga em lote deixa todas as linhas com o mesmo instante e o incremental vira full refresh sem avisar.
- Usuários separados por responsabilidade: o Airbyte só lê a origem e só escreve na bronze, o dbt lê a bronze e escreve na silver e na gold.
- Teste sentinela no dbt: valores desconhecidos de forma de pagamento caem em `NAO_MAPEADO`, e o `accepted_values` proíbe esse valor, para a mudança na origem virar uma falha visível.
- Dagster montando os assets a partir do manifesto do dbt, mais um asset para o sync do Airbyte.
- `dbt source freshness` usando `_airbyte_extracted_at`.

## Estrutura

```
infra/                       docker-compose (origem, warehouse, pgAdmin), scripts de init e valores do Airbyte
  init/origem/               schema da origem (sujo de propósito) e usuário de leitura
  init/warehouse/            schemas bronze, silver e gold e permissões
ingestao/
  gerar_dados_sinteticos.py  popula a origem com dados fictícios
  copiar_para_bronze.py      atalho que faz o papel do Airbyte
dbt_medalhao/                modelos silver e gold, testes e macros
orquestracao/medalhao/       projeto Dagster (assets do Airbyte e do dbt, jobs e schedule)
```

## Como rodar

Você precisa de Docker (Docker Desktop no Mac e no Windows), Python 3.10 ou mais novo e Git. O Airbyte é opcional: dá para ver o dbt e o Dagster funcionando sem ele.

### 1. Clonar e configurar

```sh
git clone https://github.com/rafaelmatos1512/pipeline-medalhao-seguros.git
cd pipeline-medalhao-seguros
cp .env.example .env
```

As senhas do `.env.example` batem com o docker-compose e são só de demonstração. Troque todas antes de expor qualquer coisa fora do seu computador.

### 2. Subir os bancos no Docker

```sh
docker compose -f infra/docker-compose.yml up -d
```

Isso sobe três containers:

- `pg_origem` na porta 5433: o sistema legado simulado, já com as tabelas e o usuário de leitura do Airbyte.
- `pg_warehouse` na porta 5434: o warehouse, já com os schemas bronze, silver e gold e um usuário para cada ferramenta.
- `pgadmin` em http://localhost:5050 (login `admin@local.dev`, senha `admin123`) para olhar os dados.

Os scripts de `infra/init` só rodam na primeira vez que o volume é criado. Para começar do zero: `docker compose -f infra/docker-compose.yml down -v`.

### 3. Ambiente Python

```sh
python3 -m venv .venv
source .venv/bin/activate          # no Windows: .venv\Scripts\activate
pip install -e orquestracao        # Dagster, dagster-dbt e dbt-postgres
pip install -r ingestao/requirements.txt
cd dbt_medalhao && dbt deps && cd ..
```

### 4. Gerar dados fictícios

```sh
python ingestao/gerar_dados_sinteticos.py
```

Cria 5 corretoras, 600 propostas e 125 parcelas em aberto. Dá para mudar com `--corretoras`, `--propostas` e `--seed`.

### 5. Levar os dados para a bronze

Escolha um dos caminhos.

Caminho rápido, sem Airbyte: copia a origem para a bronze no mesmo formato que o Airbyte deixaria.

```sh
python ingestao/copiar_para_bronze.py
```

Caminho completo, com Airbyte local. O Airbyte roda no Docker através do abctl, a ferramenta oficial (guia: https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart). Ele precisa de uns 8 GB de memória livres para o Docker.

```sh
abctl local install                # primeira vez, demora alguns minutos
abctl local credentials            # e-mail, senha, client id e client secret
```

Abra http://localhost:8000 e crie:

- Fonte Postgres: host `host.docker.internal`, porta 5433, banco `origem_seguradoras`, usuário `airbyte_leitor`, senha `leitor123`, schema `origem`.
- Destino Postgres: host `host.docker.internal`, porta 5434, banco `dw_medalhao`, usuário `airbyte_user`, senha `airbyte123`, schema `bronze`.
- Conexão entre os dois com as tabelas `corretoras`, `propostas` e `pendencias`, no modo Incremental Append + Deduped, cursor `atualizado_em`.

No Linux, troque `host.docker.internal` pelo IP da sua máquina na rede do Docker. Depois copie o id da conexão (está na URL da página dela) e o client id e secret para o `.env`. Com isso o Dagster consegue disparar o sync.

O `infra/subir_ambiente.sh` sobe os bancos e religa o Airbyte depois que o Docker reinicia. Foi escrito para macOS.

### 6. Rodar o dbt sozinho

```sh
cd dbt_medalhao
export DBT_HOST=localhost DBT_PORT=5434 DBT_USER=dbt_user DBT_PASSWORD=dbt123 DBT_DBNAME=dw_medalhao
dbt build                # cria silver e gold e roda todos os testes
dbt source freshness     # confere se a bronze está atualizada
dbt docs generate && dbt docs serve   # documentação e grafo de linhagem no navegador
cd ..
```

### 7. Orquestrar com o Dagster

```sh
cd orquestracao
dagster dev
```

Abra http://localhost:3000. Em Assets aparece o grafo inteiro, da bronze até a gold, montado a partir do dbt. Os jobs disponíveis:

- `pipeline_medalhao`: sync do Airbyte e depois o dbt.
- `apenas_ingestao_airbyte`: só o sync.
- `apenas_transformacoes`: só o dbt. Use este se escolheu o caminho rápido no passo 5.
- `apenas_camada_gold`: só os modelos gold.

O schedule `pipeline_diario` roda o pipeline completo às 6h; ele vem desligado e você liga na interface.

### Ver o incremental funcionando

```sh
python ingestao/gerar_dados_sinteticos.py --atualizar
```

Isso muda 10 propostas. No próximo sync do Airbyte, só essas linhas são lidas, por causa do trigger em `atualizado_em`.

## O que eu testei

Em um ambiente limpo: docker-compose subindo os dois bancos com os scripts de init, o gerador de dados, a cópia para a bronze com o usuário do Airbyte, o `dbt build` com o usuário do dbt (75 modelos e testes passando), o `dbt source freshness` e a validação das definições do Dagster com os quatro jobs e o schedule. A configuração da conexão pela interface do Airbyte segue os passos acima, mas não a refiz nesta versão. Se algo não bater no seu ambiente, abra uma issue.

## Licença

MIT.
