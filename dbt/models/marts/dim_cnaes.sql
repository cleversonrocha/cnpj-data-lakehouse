{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_cnaes')
    ]
) }}

SELECT
    CAST(ROW_NUMBER() OVER(order by codigo) AS INTEGER) AS sk_id,    
    codigo,
    CASE 
        WHEN LENGTH(codigo) = 7 THEN
            SUBSTR(codigo, 1, 2) || '.' ||
            SUBSTR(codigo, 3, 2) || '-' ||
            SUBSTR(codigo, 5, 1) || '-' ||
            SUBSTR(codigo, 6, 2)
        ELSE codigo 
    END AS codigo_formatado,
    descricao
FROM {{ ref('stg_cnaes') }}