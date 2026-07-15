-- {{ ref('fact_empresas') }} ← comentário que força dependência
-- tests/assert_quantidade_socios_fact_empresas.sql
SELECT
    sk_id,
    qtd_socios,
    qtd_socios_pf + qtd_socios_pj + qtd_socios_estrangeiro as soma_partes
FROM {{ ref('fact_empresas') }}
WHERE qtd_socios != qtd_socios_pf + qtd_socios_pj + qtd_socios_estrangeiro