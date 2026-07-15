-- {{ ref('stg_estabelecimentos') }} {{ ref('dim_estabelecimentos') }} {{ ref('fact_estabelecimentos') }} ← comentário que força dependência
-- tests/assert_quantidade_estabelecimentos
WITH stg_estabelecimentos AS (
    SELECT COUNT(sk_id) AS qtd FROM {{ ref('stg_estabelecimentos') }}
),
dim_estabelecimentos AS (
    SELECT COUNT(sk_id) AS qtd FROM {{ ref('dim_estabelecimentos') }}
),
fact_estabelecimentos AS (
    SELECT COUNT(sk_id) AS qtd FROM {{ ref('fact_estabelecimentos') }}
)

SELECT
    stg_estabelecimentos.qtd AS qtd_stg,
    dim_estabelecimentos.qtd AS qtd_dim,
    fact_estabelecimentos.qtd AS qtd_fact
FROM stg_estabelecimentos
CROSS JOIN dim_estabelecimentos
CROSS JOIN fact_estabelecimentos
WHERE stg_estabelecimentos.qtd != dim_estabelecimentos.qtd OR stg_estabelecimentos.qtd != fact_estabelecimentos.qtd