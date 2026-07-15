-- tests/assert_unique_estabelecimentos.sql
-- Falha se houver qualquer combinação (cnpj_basico, cnpj_ordem, cnpj_dv) duplicada
SELECT
    column00,
    column01,
    column02,
    COUNT(*) as qtd
FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }}
GROUP BY column00, column01, column02
HAVING qtd > 1