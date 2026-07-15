-- {{ ref('stg_estabelecimentos') }} {{ ref('dim_cnaes') }} ref('int_cnaes_principais') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_cnaes_secundarias')
    ]
) }}

SELECT    
    cs.sk_estabelecimento_id,
    c.sk_id AS sk_cnae_id,
    'SECUNDÁRIO' AS tipo_cnae
FROM (
    SELECT         
        sk_id AS sk_estabelecimento_id,        
        UNNEST(list_distinct(string_split(cnae_fiscal_secundaria, ','))) AS cnae_codigo
    FROM {{ ref('stg_estabelecimentos') }}
    WHERE cnae_fiscal_secundaria IS NOT NULL
) AS cs
JOIN {{ ref('dim_cnaes') }} c ON c.codigo = cs.cnae_codigo
LEFT JOIN {{ ref('int_cnaes_principais') }} cp ON cp.sk_estabelecimento_id = cs.sk_estabelecimento_id AND cp.sk_cnae_id = c.sk_id
WHERE cp.sk_estabelecimento_id IS NULL