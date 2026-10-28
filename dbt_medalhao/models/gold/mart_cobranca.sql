{{ config(materialized='view', schema='gold') }}

/* View operacional: dados de contato são destinados somente à equipe autorizada. */
select
    p.tenant_id,
    c.nome_corretora,
    p.nome_cliente,
    p.cpf_cnpj,
    p.telefone as whatsapp,
    p.email,
    p.produto,
    p.numero_certificado,
    p.competencia,
    p.data_vencimento,
    p.valor_pendente,
    p.dias_atraso,
    p.faixa_atraso
from {{ ref('fct_pendencias') }} p
left join {{ ref('dim_corretora') }} c
    on p.broker_id = c.broker_id
