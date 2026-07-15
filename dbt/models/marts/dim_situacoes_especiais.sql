-- {{ ref('stg_situacoes_especiais') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_situacoes_especiais')
    ]
) }}

SELECT sk_id,codigo,descricao FROM {{ ref('stg_situacoes_especiais') }}