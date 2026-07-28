{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_cnaes')
    ]
) }}

SELECT 
    CAST(ROW_NUMBER() OVER(ORDER BY codigo) AS SMALLINT) AS sk_id,
    codigo,
    codigo_formatado,
    descricao,
    NOW() AS data_processamento 
FROM {{ ref('int_cnaes') }}