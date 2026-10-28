{{ config(materialized='table', schema='gold') }}

select
    p.pendencia_id,
    p.corretora_id as broker_id,
    p.corretora_id as tenant_id,
    p.documento as cpf_cnpj,
    p.numero_certificado,
    p.numero_proposta,
    p.nome_cliente,
    p.linha_negocio,
    p.produto,
    p.competencia,
    p.data_vencimento,
    p.valor_pendente,
    p.dias_atraso,
    p.forma_pagamento,
    p.status_pagamento,
    p.email,
    p.telefone,
    case
        when p.dias_atraso <= 15 then '01_ATE_15_DIAS'
        when p.dias_atraso <= 30 then '02_16_A_30_DIAS'
        when p.dias_atraso <= 60 then '03_31_A_60_DIAS'
        else '04_MAIS_60_DIAS'
    end as faixa_atraso,
    p.carregado_em
from {{ ref('stg_pendencias') }} p
