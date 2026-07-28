{{ config(
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='stg_simples')
    ]
) }}

SELECT    
    column0 AS cnpj_basico,
    column1 AS opcao_simples,    
    TRY_CAST(try_strptime(column2, '%Y%m%d') AS DATE) AS data_opcao_simples,        
    TRY_CAST(try_strptime(column3, '%Y%m%d') AS DATE) AS data_exclusao_simples,
    column4 AS opcao_mei,        
    TRY_CAST(try_strptime(column5, '%Y%m%d') AS DATE) AS data_opcao_mei,     
    TRY_CAST(try_strptime(column6, '%Y%m%d') AS DATE) AS data_exclusao_mei,    
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='simples') }}
