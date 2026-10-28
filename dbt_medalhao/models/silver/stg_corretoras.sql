{{
    config(
        materialized = 'view',
        schema       = 'silver'
    )
}}

/* Cadastro versionado usado pelo conector para percorrer os quatro grupos e
   todas as corretoras vinculadas. Uma linha por CNPJ. */

with bronze as (

    select
        cnpj::text,
        cnpj_limpo::text,
        razao_social::text,
        tipo::text,
        _airbyte_extracted_at as carregado_em,
        row_number() over (
            partition by cnpj_limpo
            order by _airbyte_extracted_at desc
        ) as ordem_recente
    from {{ source('bronze', 'corretoras') }}

)

select
    lpad(regexp_replace(cnpj_limpo, '[^0-9]', '', 'g'), 14, '0') as corretora_id,
    lpad(regexp_replace(cnpj_limpo, '[^0-9]', '', 'g'), 14, '0') as cnpj,
    cnpj                                                          as cnpj_formatado,
    initcap(trim(razao_social))                                   as razao_social,
    lower(trim(tipo))                                              as tipo,
    carregado_em
from bronze
where ordem_recente = 1
