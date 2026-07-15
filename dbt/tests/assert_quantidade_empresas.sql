-- {{ ref('stg_empresas') }} {{ ref('dim_empresas') }} {{ ref('fact_empresas') }} ← comentário que força dependência
-- tests/assert_quantidade_empresas
WITH stg_empresas AS (
    SELECT COUNT(sk_id) AS qtd FROM {{ ref('stg_empresas') }}
),
dim_empresas AS (
    SELECT COUNT(sk_id) AS qtd FROM {{ ref('dim_empresas') }}
),
fact_empresas AS (
    SELECT COUNT(sk_id) AS qtd FROM {{ ref('fact_empresas') }}
)

SELECT
    stg_empresas.qtd AS qtd_stg,
    dim_empresas.qtd AS qtd_dim,
    fact_empresas.qtd AS qtd_fact
FROM stg_empresas
CROSS JOIN dim_empresas
CROSS JOIN fact_empresas
WHERE stg_empresas.qtd != dim_empresas.qtd OR stg_empresas.qtd != fact_empresas.qtd