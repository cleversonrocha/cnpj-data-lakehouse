-- {{ ref('stg_socios') }} {{ ref('dim_socios') }} {{ ref('fact_socios') }}← comentário que força dependência
-- tests/assert_quantidade_socios
WITH stg_socios AS (
    SELECT COUNT(sk_id) AS qtd FROM {{ ref('stg_socios') }}
),
dim_socios AS (
    SELECT COUNT(sk_id) AS qtd FROM {{ ref('dim_socios') }}
),
fact_socios AS (
    SELECT COUNT(sk_id) AS qtd FROM {{ ref('fact_socios') }}
)

SELECT
    stg_socios.qtd AS qtd_stg,
    dim_socios.qtd AS qtd_dim,
    fact_socios.qtd AS qtd_fact
FROM stg_socios
CROSS JOIN dim_socios
CROSS JOIN fact_socios
WHERE stg_socios.qtd != dim_socios.qtd OR stg_socios.qtd != fact_socios.qtd