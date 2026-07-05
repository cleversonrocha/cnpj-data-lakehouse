-- {{ ref('dim_cnaes') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_cnaes_principais')
    ]
) }}

SELECT 
    e.sk_id AS sk_estabelecimento_id,
    c.sk_id AS sk_cnae_id,
    'PRINCIPAL' AS tipo_cnae
FROM {{ get_s3_path(base_path='silver/cleaned', file_name='estabelecimentos') }} e
JOIN {{ ref('dim_cnaes') }} c ON c.codigo = e.cnae_fiscal_principal