-- {{ ref('stg_ufs') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_ufs')
    ]
) }}

SELECT sk_id,codigo,sigla,descricao,regiao FROM {{ ref('stg_ufs') }}