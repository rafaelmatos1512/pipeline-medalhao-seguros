{{ config(materialized='view', schema='gold') }}

with propostas as (
    select
        corretora_id as broker_id,
        count(*) as total_propostas,
        count(*) filter (where is_convertida) as propostas_convertidas,
        coalesce(sum(valor) filter (where is_convertida), 0) as valor_convertido
    from {{ ref('fct_propostas') }}
    group by 1
),
pendencias as (
    select
        broker_id,
        count(*) as total_pendencias,
        coalesce(sum(valor_pendente), 0) as valor_em_atraso
    from {{ ref('fct_pendencias') }}
    group by 1
)
select
    c.tenant_id,
    c.broker_id,
    c.nome_corretora,
    coalesce(pr.total_propostas, 0) as total_propostas,
    coalesce(pr.propostas_convertidas, 0) as propostas_convertidas,
    coalesce(pr.valor_convertido, 0) as valor_convertido,
    coalesce(pe.total_pendencias, 0) as total_pendencias,
    coalesce(pe.valor_em_atraso, 0) as valor_em_atraso
from {{ ref('dim_corretora') }} c
left join propostas pr on c.broker_id = pr.broker_id
left join pendencias pe on c.broker_id = pe.broker_id
