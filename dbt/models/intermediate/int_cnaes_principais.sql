{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_cnaes_principais')
    ]
) }}

SELECT 
    e.sk_id AS sk_estabelecimento,
    c.codigo AS fk_cnae,
    'PRINCIPAL' AS tipo_cnae,
    NOW() AS data_processamento
FROM {{ ref('int_estabelecimentos') }} e
JOIN {{ ref('int_cnaes') }} c ON c.codigo = e.cnae_fiscal_principal