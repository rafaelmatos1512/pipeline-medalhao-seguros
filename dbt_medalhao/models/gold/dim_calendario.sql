WITH datas AS (
        SELECT generate_series(
            '2020-01-01'::DATE,
            '2030-12-31'::DATE,
            '1 day'::interval
        )::DATE AS data_completa
    )
    SELECT
        to_char(data_completa, 'YYYYMMDD')::INT AS id_calendario,
        data_completa,
        EXTRACT(YEAR FROM data_completa) AS ano,
        EXTRACT(MONTH FROM data_completa) AS mes,
        EXTRACT(QUARTER FROM data_completa) AS trimestre,
        to_char(data_completa, 'Day') AS nome_dia
    FROM datas