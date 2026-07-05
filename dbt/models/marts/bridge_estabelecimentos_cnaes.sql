-- {{ ref('int_cnaes_principais') }} {{ ref('int_cnaes_secundarias') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/bridge', file_name='bridge_estabelecimentos_cnaes')
    ]
) }}

SELECT sk_estabelecimento_id, sk_cnae_id, tipo_cnae FROM {{ ref('int_cnaes_principais') }}
UNION ALL
SELECT sk_estabelecimento_id, sk_cnae_id, tipo_cnae FROM {{ ref('int_cnaes_secundarias') }}