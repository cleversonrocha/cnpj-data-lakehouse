{{ config(    
    post_hook=[        
        export_to_s3(bucket_path='gold/int', file_name='int_tempo_inicio_atividades')
    ]
) }}

WITH date_spine AS (
    -- No DuckDB, generate_series é uma função de tabela nativa e super rápida
    SELECT CAST(generate_series AS DATE) AS data_referencia
    FROM generate_series((SELECT MIN(data_inicio_atividade) FROM {{ ref('int_estabelecimentos') }} WHERE data_inicio_atividade IS NOT NULL), 
                         (SELECT MAX(data_inicio_atividade) FROM {{ ref('int_estabelecimentos') }} WHERE data_inicio_atividade IS NOT NULL),
                         INTERVAL '1 day')
),

dim_tempo_enriquecida AS (
    SELECT
        -- O DuckDB possui STRFTIME, o que deixa a criação do ID numérico muito mais limpa e performática
        CAST(STRFTIME(data_referencia, '%Y%m%d') AS INTEGER) AS sk_id,
        data_referencia,
        CAST(EXTRACT(YEAR FROM data_referencia) AS SMALLINT) AS ano,
        CAST(EXTRACT(MONTH FROM data_referencia) AS TINYINT) AS mes,
        CAST(EXTRACT(DAY FROM data_referencia) AS TINYINT) AS dia,
        CAST(EXTRACT(QUARTER FROM data_referencia) AS TINYINT) AS trimestre,    
        
        -- Semestre
        CAST(
            CASE 
                WHEN EXTRACT(MONTH FROM data_referencia) <= 6 THEN 1 
                ELSE 2 
            END AS TINYINT
        ) AS semestre,

        -- Dia da semana usando ISO (1 = Segunda-feira, 7 = Domingo)
        CAST(EXTRACT(ISODOW FROM data_referencia) AS TINYINT) AS dia_semana_numero,

        -- Nomes dos meses em Português
        CASE EXTRACT(MONTH FROM data_referencia)
            WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
            WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
            WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
            WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
        END AS nome_mes,        

        -- Nome dos dias da semana em Português
        CASE EXTRACT(ISODOW FROM data_referencia)
            WHEN 1 THEN 'Segunda-feira'
            WHEN 2 THEN 'Terça-feira'
            WHEN 3 THEN 'Quarta-feira'
            WHEN 4 THEN 'Quinta-feira'
            WHEN 5 THEN 'Sexta-feira'
            WHEN 6 THEN 'Sábado'
            WHEN 7 THEN 'Domingo'
        END AS nome_dia_semana

    FROM date_spine
)

SELECT
    sk_id,
    data_referencia,
    ano,
    mes,
    dia,
    trimestre,
    semestre,
    dia_semana_numero,
    nome_mes,
    nome_dia_semana,
    NOW() AS data_processamento
FROM dim_tempo_enriquecida

UNION ALL

SELECT 
    -1 AS sk_id,
    CAST(NULL AS DATE) AS data_referencia,
    CAST(NULL AS SMALLINT) AS ano,
    CAST(NULL AS TINYINT) AS mes,
    CAST(NULL AS TINYINT) AS dia,
    CAST(NULL AS TINYINT) AS trimestre,
    CAST(NULL AS TINYINT) AS semestre,
    CAST(NULL AS TINYINT) AS dia_semana_numero,
    'Não informado' AS nome_mes,
    'Não informado' AS nome_dia_semana,    
    NOW() AS data_processamento