{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

/*
================================================================================
  fct_propostas — camada GOLD (tabela FATO)

  Uma linha por proposta, com as MÉTRICAS e as CLASSIFICAÇÕES DE NEGÓCIO.

  O QUE FAZ: aplica regra de negócio.
  O QUE NÃO FAZ: limpeza. Isso já aconteceu na silver — aqui não existe
                 regexp_replace, não existe to_date, não existe initcap.

  ── POR QUE ISTO É "table" E NÃO "view" ─────────────────────────────────────
  A silver é view porque limpeza é barata e queremos o dado sempre fresco.
  A gold é tabela porque é consultada muitas vezes (BI, relatório, aplicação)
  e carrega JOINs e CASEs. Vale pagar o custo uma vez na construção em vez de
  a cada consulta.
================================================================================
*/

with

propostas as (

    -- ref() em vez de source(): estamos lendo um modelo que o próprio dbt
    -- construiu. É isso que monta o grafo de dependências — o dbt sabe que
    -- precisa construir stg_propostas ANTES deste modelo, sem ninguém dizer.
    select * from {{ ref('stg_propostas') }}

),

corretoras as (

    select * from {{ ref('stg_corretoras') }}

),

com_regras as (

    select

        p.proposta_id,
        p.numero_proposta,
        p.corretora_id,
        c.razao_social                          as corretora,
        c.cnpj                                  as corretora_cnpj,

        p.documento,
        p.tipo_documento,
        p.nome_cliente,

        p.produto,
        p.linha_negocio,
        p.forma_pagamento,

        p.data_criacao,
        p.data_vencimento,
        p.valor,

        p.status_proposta,
        p.status_pagamento,
        p.motivo_pendencia,

        -- ═══════════════════════════════════════════════════════════════
        --  REGRA 1 — DIAS DE ATRASO
        --
        --  Substitui calculate_delay_days() do Python.
        --  Só conta atraso do que já venceu; futuro é zero, não negativo.
        -- ═══════════════════════════════════════════════════════════════
        case
            when p.data_vencimento is null then 0
            else greatest(0, (CURRENT_DATE - p.data_vencimento))
        end                                                   as dias_atraso,

        -- ═══════════════════════════════════════════════════════════════
        --  REGRA 2 — INADIMPLÊNCIA
        --
        --  ⚠️ ESTA É A DEFINIÇÃO MAIS IMPORTANTE DO PROJETO, e é uma
        --     decisão de NEGÓCIO, não técnica. Está aqui, num lugar só,
        --     versionada — em vez de espalhada por vários relatórios.
        --
        --  Definição adotada: é inadimplente quando
        --     (a) o pagamento está PENDENTE,
        --     (b) a data de vencimento já passou, e
        --     (c) a proposta NÃO foi recusada nem cancelada.
        --
        --  O item (c) é o que evita o erro clássico. Num exemplo:
        --     PENDENTE + RECUSADA  = propostas que nunca serão cobradas
        --  Essas nunca vão ser pagas porque a proposta MORREU — contá-las
        --  como inadimplência infla o indicador e mistura dois problemas
        --  diferentes: "cliente não paga" e "proposta não foi aceita".
        --
        --  👉 VALIDE ESTA REGRA COM A ÁREA DE NEGÓCIO antes de usar em
        --     produção. Se a definição mudar, muda AQUI e todos os
        --     relatórios acompanham.
        -- ═══════════════════════════════════════════════════════════════
        coalesce((
            p.status_pagamento = 'PENDENTE'
            and p.data_vencimento < CURRENT_DATE
            and p.status_proposta not in ('RECUSADA', 'CANCELADA')
        ), false)                                              as is_inadimplente,

        -- ═══════════════════════════════════════════════════════════════
        --  REGRA 3 — CONVERSÃO
        --  Proposta que virou negócio de fato: paga E implantada.
        -- ═══════════════════════════════════════════════════════════════
        (
            p.status_pagamento = 'QUITADA'
            and p.status_proposta = 'IMPLANTADA'
        )                                                     as is_convertida,

        -- ═══════════════════════════════════════════════════════════════
        --  REGRA 4 — PERDA
        --  Proposta que morreu: não vira receita nem cobrança.
        -- ═══════════════════════════════════════════════════════════════
        (p.status_proposta in ('RECUSADA', 'CANCELADA'))      as is_perdida,

        -- ═══════════════════════════════════════════════════════════════
        --  REGRA 5 — SITUAÇÃO CONSOLIDADA
        --
        --  Uma coluna, um valor por proposta. Existe porque os dois status
        --  da origem se contradizem (ex.: CANCELADA + IMPLANTADA)
        --  e ninguém consegue montar relatório com dois eixos ambíguos.
        --
        --  A ORDEM DOS CASE IMPORTA: o primeiro que casar vence. Perda vem
        --  antes de inadimplência justamente para que proposta recusada não
        --  seja contada como caloteira.
        -- ═══════════════════════════════════════════════════════════════
        --  Os últimos casos surgiram do teste-sentinela: ao proibir 'OUTROS'
        --  nos accepted_values, o build falhou e mostrou combinações que a
        --  regra original não previa. Sem esse teste, elas ficariam num balde
        --  genérico que ninguém olha.
        case
            when p.status_proposta in ('RECUSADA', 'CANCELADA')
                then 'PERDIDA'
            when p.status_pagamento = 'QUITADA'
                then 'PAGA'
            when p.status_pagamento = 'PENDENTE' and p.data_vencimento < CURRENT_DATE
                then 'INADIMPLENTE'
            when p.status_pagamento = 'PENDENTE'
                then 'A_VENCER'

            -- Pagamento recorrente em curso. Saudável, mas não é
            -- "PAGA" definitiva: ainda há parcelas por vir.
            when p.status_pagamento = 'ATIVA'
                then 'ATIVA'

            -- Apólice IMPLANTADA com pagamento CANCELADO.
            --    Não é inadimplência: o cliente não deixou de pagar, a
            --    cobrança é que foi cancelada. Contrato vigente sem receita.
            when p.status_pagamento = 'CANCELADA'
                then 'PAGAMENTO_CANCELADO'

            -- Estorno/devolução.
            when p.status_pagamento = 'DEVOLVIDA'
                then 'DEVOLVIDA'

            -- A origem usa '-' quando não informa a situação de pagamento.
            when p.status_pagamento in ('-', 'NAO INFORMADO')
              or p.status_pagamento is null
                then 'SEM_STATUS_PAGAMENTO'

            -- Sem `else`: qualquer status novo vira NULL e o teste not_null
            -- quebra o build. Preferimos falhar alto a classificar errado.
        end                                                   as situacao,

        -- ═══════════════════════════════════════════════════════════════
        --  REGRA 6 — FAIXA DE ATRASO
        --  Faixas de cobrança. A ação muda conforme o tempo: lembrete,
        --  ligação, negativação. Por isso a faixa, e não só o número.
        -- ═══════════════════════════════════════════════════════════════
        case
            when p.data_vencimento is null                          then 'SEM_VENCIMENTO'
            when p.data_vencimento >= CURRENT_DATE                  then 'A_VENCER'
            when (CURRENT_DATE - p.data_vencimento) <= 30           then '01_ATE_30_DIAS'
            when (CURRENT_DATE - p.data_vencimento) <= 60           then '02_31_A_60_DIAS'
            when (CURRENT_DATE - p.data_vencimento) <= 90           then '03_61_A_90_DIAS'
            else                                                         '04_MAIS_90_DIAS'
        end                                                   as faixa_atraso,

        -- Linhagem: até onde o dado veio
        p.carregado_em

    from propostas p
    -- LEFT JOIN, não INNER: se uma corretora sumir da origem, a proposta
    -- continua no fato (com a corretora nula) em vez de desaparecer do
    -- relatório silenciosamente. Perder linha em JOIN é o erro mais comum
    -- e mais difícil de perceber em modelagem.
    left join corretoras c on c.corretora_id = p.corretora_id

)

select * from com_regras
