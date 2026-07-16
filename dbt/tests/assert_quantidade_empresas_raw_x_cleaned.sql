-- {{ ref('stg_empresas') }} {{ ref('int_empresas_nao_qualificadas') }} ← comentário que força dependência
-- tests/assert_quantidade_empresas

WITH stg_empresas_raw AS (
    SELECT COUNT(column0) AS qtd_raw FROM {{ get_s3_path(base_path='silver/raw', file_name='empresas') }}
),
int_empresas_nao_qualificadas AS (
    SELECT COUNT(column0) AS qtd_nao_qualificadas FROM {{ get_s3_path(base_path='silver/raw', file_name='int_empresas_nao_qualificadas') }}CT 
),
stg_empresas_cleaned AS (
    SELECT COUNT(cnpj_basico) AS qtd_cleaned FROM {{ ref('stg_empresas') }}
)

SELECT qtd_raw, qtd_nao_qualificadas, qtd_cleaned
FROM stg_empresas_raw
CROSS JOIN int_empresas_nao_qualificadas
CROSS JOIN stg_empresas_cleaned
WHERE (qtd_raw-qtd_nao_qualificadas) != qtd_cleaned