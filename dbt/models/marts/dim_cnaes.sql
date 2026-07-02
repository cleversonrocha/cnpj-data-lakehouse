{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_cnaes')
    ]
) }}

SELECT
    CAST(ROW_NUMBER() OVER(order by codigo) AS INTEGER) AS sk_id,    
    codigo,
    descricao
FROM {{ get_s3_path(base_path='silver/cleaned', file_name='cnaes') }}