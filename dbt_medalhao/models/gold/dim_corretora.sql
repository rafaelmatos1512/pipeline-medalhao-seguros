{{ config(materialized='table', schema='gold') }}

select
    corretora_id as broker_id,
    corretora_id as tenant_id,
    cnpj,
    razao_social as nome_corretora,
    tipo,
    carregado_em
from {{ ref('stg_corretoras') }}
