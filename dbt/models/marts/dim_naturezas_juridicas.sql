-- {{ ref('stg_naturezas') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_naturezas_juridicas')
    ]
) }}

SELECT sk_id,codigo,codigo_formatado,descricao FROM {{ ref('stg_naturezas') }}