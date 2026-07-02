-- {{ ref('dim_cnaes') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/bridge', file_name='bridge_estabelecimentos_cnaes')
    ]
) }}

WITH lista_cnaes_principal AS (
    SELECT        
        sk_id AS sk_estabelecimento_id,
        cnae_fiscal_principal as sk_cnae_id,
        'PRINCIPAL' AS tipo_cnae
    FROM
        {{ get_s3_path(base_path='silver/cleaned', file_name='estabelecimentos') }}
),

lista_cnaes_secundaria AS (
    SELECT        
        sk_id AS sk_estabelecimento_id,
        cnae_codigo AS sk_cnae_id,
        'SECUNDÁRIO' AS tipo_cnae
    FROM
        {{ get_s3_path(base_path='silver/cleaned', file_name='estabelecimentos') }},
        UNNEST(
            list_distinct(string_split(cnae_fiscal_secundaria, ','))
        ) AS t(cnae_codigo)
    WHERE
        cnae_fiscal_secundaria IS NOT NULL
)

SELECT sk_estabelecimento_id, c.sk_id as sk_cnae_id, tipo_cnae 
FROM lista_cnaes_principal p
JOIN {{ ref('dim_cnaes') }} c ON c.codigo = p.sk_cnae_id

UNION ALL

SELECT s.sk_estabelecimento_id, c.sk_id as sk_cnae_id, s.tipo_cnae 
FROM lista_cnaes_secundaria s
JOIN {{ ref('dim_cnaes') }} c ON c.codigo = s.sk_cnae_id
LEFT JOIN lista_cnaes_principal p ON p.sk_estabelecimento_id = s.sk_estabelecimento_id AND p.sk_cnae_id = s.sk_cnae_id
WHERE p.sk_estabelecimento_id IS NULL