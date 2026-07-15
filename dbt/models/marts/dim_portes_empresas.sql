-- {{ ref('stg_portes_empresas') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_portes_empresas')
    ]
) }}

SELECT sk_id,codigo,sigla,descricao FROM {{ ref('stg_portes_empresas') }}