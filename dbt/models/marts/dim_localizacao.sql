{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_localizacao')
    ]
) }}

WITH localizacao AS (
    SELECT
        DISTINCT    
            uf,        
            desc_municipio,            
            pais,
            desc_pais
    FROM {{ get_s3_path(base_path='silver/cleaned', file_name='estabelecimentos') }}
)

SELECT 
    CAST(ROW_NUMBER() OVER(order by uf,desc_municipio,pais,desc_pais) AS INTEGER) AS sk_id,
    uf,
    desc_municipio,    
    pais,
    desc_pais
FROM localizacao