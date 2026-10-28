{{ config(materialized='table') }}

    SELECT
        c.nome_corretora,
        c.cnpj,
        COUNT(p.proposta_id) AS total_propostas,
        SUM(p.valor) AS valor_total_propostas,
        SUM(CASE WHEN p.is_convertida THEN p.valor ELSE 0 END) AS valor_total_pago,
        SUM(CASE WHEN p.is_inadimplente THEN p.valor ELSE 0 END) AS valor_inadimplente
    FROM {{ ref('fct_propostas') }} p
    LEFT JOIN {{ ref('dim_corretora') }} c
        ON p.corretora_id = c.broker_id
    GROUP BY
        c.nome_corretora,
        c.cnpj