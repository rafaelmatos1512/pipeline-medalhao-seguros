{{
    config(
        materialized = 'view',
        schema       = 'silver'
    )
}}

/*
  Uma linha por proposta da origem. A silver apenas limpa, tipa,
  padroniza nomes e remove duplicatas; regra de negocio fica na gold.
*/

with bronze as (

    select
        numero_proposta::text             as numero_proposta_raw,
        nome_cliente::text             as nome_raw,
        documento_cliente::text              as documento_raw,
        produto::text                as produto_raw,
        linha_negocio::text               as linha_negocio_raw,
        data_criacao::text              as data_criacao_raw,
        data_referencia::text                 as data_referencia_raw,
        data_vencimento::text         as data_vencimento_raw,
        forma_pagamento::text             as forma_pagamento_raw,
        status_proposta::text                 as status_proposta_raw,
        status_pagamento::text            as status_pagamento_raw,
        motivo_pendencia::text            as motivo_pendencia_raw,
        valor::numeric           as valor_raw,
        corretora_cnpj::text                as corretora_cnpj_raw,
        corretora_razao_social::text        as corretora_razao_social_raw,
        _airbyte_extracted_at               as carregado_em,
        row_number() over (
            partition by corretora_cnpj, numero_proposta
            order by _airbyte_extracted_at desc
        )                                   as ordem_recente
    from {{ source('bronze', 'propostas') }}

),

preparado as (

    select
        nullif(trim(numero_proposta_raw), '')                    as numero_proposta,
        lpad(regexp_replace(coalesce(corretora_cnpj_raw, ''), '[^0-9]', '', 'g'), 14, '0')
                                                                 as corretora_id,
        nullif(regexp_replace(documento_raw, '[^0-9]', '', 'g'), '')
                                                                 as documento_digitos,
        nullif(trim(data_criacao_raw), '')                       as data_criacao_texto,
        nullif(trim(data_referencia_raw), '')                    as data_referencia_texto,
        nullif(trim(data_vencimento_raw), '')                    as data_vencimento_texto,
        *,
        translate(
            upper(trim(coalesce(forma_pagamento_raw, ''))),
            'ÁÀÂÃÉÊÍÓÔÕÚÇ',
            'AAAAEEIOOOUC'
        )                                                        as forma_pagamento_normalizada
    from bronze
    where ordem_recente = 1

),

limpo as (

    select
        md5(corretora_id || '|' || numero_proposta)             as proposta_id,
        corretora_id,
        numero_proposta,

        case
            when documento_digitos is null then null
            when length(documento_digitos) <= 11 then lpad(documento_digitos, 11, '0')
            else lpad(documento_digitos, 14, '0')
        end                                                      as documento,
        case
            when documento_digitos is null then null
            when length(documento_digitos) <= 11 then 'CPF'
            else 'CNPJ'
        end                                                      as tipo_documento,

        initcap(nullif(trim(nome_raw), ''))                      as nome_cliente,
        upper(nullif(trim(produto_raw), ''))                     as produto,
        case translate(
            upper(trim(coalesce(linha_negocio_raw, ''))),
            'ÁÀÂÃÉÊÍÓÔÕÚÇ',
            'AAAAEEIOOOUC'
        )
            when 'VIDA' then 'Vida'
            when 'PREVIDENCIA' then 'Previdencia'
            else initcap(nullif(trim(linha_negocio_raw), ''))
        end                                                      as linha_negocio,

        case
            when data_criacao_texto ~ '^\d{4}-\d{2}-\d{2}'
                then left(data_criacao_texto, 10)::date
            when data_criacao_texto ~ '^\d{2}/\d{2}/\d{4}'
                then to_date(left(data_criacao_texto, 10), 'DD/MM/YYYY')
        end                                                      as data_criacao,
        case
            when data_referencia_texto ~ '^\d{4}-\d{2}-\d{2}'
                then left(data_referencia_texto, 10)::date
            when data_referencia_texto ~ '^\d{2}/\d{2}/\d{4}'
                then to_date(left(data_referencia_texto, 10), 'DD/MM/YYYY')
        end                                                      as data_referencia,
        case
            when data_vencimento_texto ~ '^\d{4}-\d{2}-\d{2}'
                then nullif(left(data_vencimento_texto, 10)::date, date '1900-01-01')
            when data_vencimento_texto ~ '^\d{2}/\d{2}/\d{4}'
                then nullif(
                    to_date(left(data_vencimento_texto, 10), 'DD/MM/YYYY'),
                    date '1900-01-01'
                )
        end                                                      as data_vencimento,

        nullif(translate(
            upper(trim(status_proposta_raw)),
            'ÁÀÂÃÉÊÍÓÔÕÚÇ',
            'AAAAEEIOOOUC'
        ), '')                                                    as status_proposta,
        nullif(translate(
            upper(trim(status_pagamento_raw)),
            'ÁÀÂÃÉÊÍÓÔÕÚÇ',
            'AAAAEEIOOOUC'
        ), '')                                                    as status_pagamento,
        nullif(trim(motivo_pendencia_raw), '')                   as motivo_pendencia,

        forma_pagamento_raw                                      as forma_pagamento_origem,
        case
            when forma_pagamento_normalizada like '%CREDIT%'
              or forma_pagamento_normalizada like '%CART%'
                then 'CARTAO_CREDITO'
            when forma_pagamento_normalizada like '%DEBIT%'
                then 'DEBITO_AUTOMATICO'
            when forma_pagamento_normalizada like '%BOLETO%'
                then 'BOLETO'
            when forma_pagamento_normalizada in ('OUTRO', 'OUTROS')
                then 'OUTROS'
            else 'NAO_MAPEADO'
        end                                                      as forma_pagamento,

        coalesce(valor_raw, 0)::numeric(15,2)                    as valor,
        corretora_razao_social_raw                               as corretora_razao_social_origem,
        data_criacao_raw                                         as origem_data_criacao,
        data_referencia_raw                                      as origem_data_referencia,
        data_vencimento_raw                                      as origem_data_vencimento,
        carregado_em
    from preparado

)

select * from limpo
