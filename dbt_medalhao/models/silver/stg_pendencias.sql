{{ config(materialized='view', schema='silver') }}

/* Uma linha por parcela em aberto da origem, limpa mas sem agregação. */

with bronze as (
    select
        corretora_cnpj::text                         as corretora_cnpj_raw,
        corretora_razao_social::text                  as corretora_raw,
        numero_certificado::text                    as numero_certificado_raw,
        numero_proposta::text                       as numero_proposta_raw,
        documento_cliente::text                       as documento_raw,
        nome_cliente::text                          as nome_raw,
        linha_negocio::text                         as linha_negocio_raw,
        produto::text                 as produto_raw,
        competencia::text                          as competencia_raw,
        data_vencimento::text                   as vencimento_raw,
        valor_parcela::numeric                      as valor_raw,
        dias_atraso::integer                      as dias_atraso_raw,
        forma_pagamento::text                        as forma_pagamento_raw,
        status_pagamento::text                      as status_pagamento_raw,
        email::text                                as email_raw,
        telefone::text                            as telefone_raw,
        snapshot_id::text                            as snapshot_id,
        _airbyte_extracted_at                         as carregado_em,
        row_number() over (
            partition by corretora_cnpj, numero_certificado, competencia, data_vencimento
            order by _airbyte_extracted_at desc
        )                                             as ordem_recente
    from {{ source('bronze', 'pendencias') }}
    where snapshot_id is not null
      and snapshot_id = (
          select max(snapshot_id)
          from {{ source('bronze', 'pendencias') }}
          where snapshot_id is not null
      )
),

preparado as (
    select
        lpad(regexp_replace(coalesce(corretora_cnpj_raw, ''), '[^0-9]', '', 'g'), 14, '0') as corretora_id,
        nullif(regexp_replace(documento_raw, '[^0-9]', '', 'g'), '') as documento_digitos,
        nullif(trim(vencimento_raw), '') as vencimento_texto,
        *
    from bronze
    where ordem_recente = 1
)

select
    md5(concat_ws('|', corretora_id, coalesce(numero_certificado_raw, ''), coalesce(competencia_raw, ''), coalesce(vencimento_texto, ''))) as pendencia_id,
    corretora_id,
    nullif(trim(numero_certificado_raw), '') as numero_certificado,
    nullif(trim(numero_proposta_raw), '') as numero_proposta,
    case
        when documento_digitos is null then null
        when length(documento_digitos) <= 11 then lpad(documento_digitos, 11, '0')
        else lpad(documento_digitos, 14, '0')
    end as documento,
    initcap(nullif(trim(nome_raw), '')) as nome_cliente,
    initcap(nullif(trim(linha_negocio_raw), '')) as linha_negocio,
    upper(nullif(trim(produto_raw), '')) as produto,
    nullif(trim(competencia_raw), '') as competencia,
    case
        when vencimento_texto ~ '^\d{4}-\d{2}-\d{2}' then left(vencimento_texto, 10)::date
        when vencimento_texto ~ '^\d{2}/\d{2}/\d{4}' then to_date(left(vencimento_texto, 10), 'DD/MM/YYYY')
    end as data_vencimento,
    coalesce(valor_raw, 0)::numeric(15,2) as valor_pendente,
    greatest(coalesce(dias_atraso_raw, 0), 0) as dias_atraso,
    upper(nullif(trim(forma_pagamento_raw), '')) as forma_pagamento,
    upper(nullif(trim(status_pagamento_raw), '')) as status_pagamento,
    nullif(trim(email_raw), '') as email,
    nullif(trim(telefone_raw), '') as telefone,
    corretora_raw as corretora_razao_social_origem,
    carregado_em
from preparado
