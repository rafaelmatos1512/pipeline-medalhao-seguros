{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

/*
================================================================================
  mart_inadimplencia_corretora — camada GOLD (MART)

  Uma linha por corretora, com os indicadores prontos para relatório.

  Repare: este modelo NÃO repete nenhuma regra de negócio. Ele só agrega o que
  o fct_propostas já classificou. Se a definição de inadimplência mudar, muda
  em UM lugar (fct_propostas) e este mart acompanha sozinho.

  É essa a diferença entre ter uma camada de fato e escrever CASE WHEN em cada
  relatório.
================================================================================
*/

with

fatos as (

    select * from {{ ref('fct_propostas') }}

),

agregado as (

    select

        corretora_id,
        corretora,
        corretora_cnpj,

        -- ---- Volumes ----
        count(*)                                              as total_propostas,
        count(*) filter (where is_convertida)                 as propostas_pagas,
        count(*) filter (where is_inadimplente)                as propostas_inadimplentes,
        count(*) filter (where is_perdida)                     as propostas_perdidas,

        -- ---- Valores ----
        sum(valor)                                             as valor_total,
        sum(valor) filter (where is_convertida)                as valor_recebido,
        sum(valor) filter (where is_inadimplente)               as valor_inadimplente,
        sum(valor) filter (where is_perdida)                    as valor_perdido,

        -- ---- Indicadores ----
        -- nullif(...,0) no denominador evita divisão por zero. Sem isso, uma
        -- corretora sem propostas derruba o modelo inteiro.
        round(
            100.0 * count(*) filter (where is_inadimplente)
                  / nullif(count(*), 0)
        , 2)                                                   as taxa_inadimplencia_pct,

        round(
            100.0 * count(*) filter (where is_convertida)
                  / nullif(count(*), 0)
        , 2)                                                   as taxa_conversao_pct,

        -- Atraso médio só das que estão inadimplentes de fato. Incluir as
        -- pagas puxaria a média para baixo e esconderia o problema.
        round(avg(dias_atraso) filter (where is_inadimplente), 0)
                                                               as dias_atraso_medio,

        max(dias_atraso) filter (where is_inadimplente)         as dias_atraso_maximo,

        -- ---- Janela dos dados ----
        min(data_criacao)                                      as primeira_proposta,
        max(data_criacao)                                      as ultima_proposta,
        max(carregado_em)                                      as atualizado_em

    from fatos
    group by 1, 2, 3

)

select * from agregado
order by valor_inadimplente desc nulls last
