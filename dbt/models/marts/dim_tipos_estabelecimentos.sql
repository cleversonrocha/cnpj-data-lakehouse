-- {{ ref('stg_tipos_estabelecimentos') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_tipos_estabelecimentos')
    ]
) }}

SELECT sk_id,codigo,descricao FROM {{ ref('stg_tipos_estabelecimentos') }}