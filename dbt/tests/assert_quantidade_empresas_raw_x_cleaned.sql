-- {{ ref('stg_empresas') }} ← comentário que força dependência
-- tests/assert_quantidade_empresas

WITH stg_empresas_raw AS (
    SELECT COUNT(column0) AS qtd_raw FROM {{ get_s3_path(base_path='silver/raw', file_name='empresas') }}
),
stg_empresas_cleaned AS (
    SELECT COUNT(cnpj_basico) AS qtd_cleaned FROM {{ ref('stg_empresas') }}
)

SELECT qtd_raw,qtd_cleaned,    
FROM stg_empresas_raw
CROSS JOIN stg_empresas_cleaned
WHERE (qtd_raw - qtd_cleaned) != 1 --CNPJ DUPLICADO DE 06/2026