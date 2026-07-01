{{ config(
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='simples')
    ]
) }}

SELECT    
    CAST(column0 AS VARCHAR) AS cnpj_basico,        
    CAST(column1 AS VARCHAR) AS opcao_simples,    
    CASE 
        WHEN LENGTH(TRIM(column2)) = 8 AND column2 != '00000000'
        THEN TRY_CAST(strptime(column2, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_opcao_simples,
    CASE 
        WHEN LENGTH(TRIM(column3)) = 8 AND column2 != '00000000'
        THEN TRY_CAST(strptime(column2, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_exclusao_simples,    
    CAST(column4 AS VARCHAR) AS opcao_mei,    
    CASE 
        WHEN LENGTH(TRIM(column5)) = 8 AND column5 != '00000000'
        THEN TRY_CAST(strptime(column5, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_opcao_mei,      
    CASE 
        WHEN LENGTH(TRIM(column6)) = 8 AND column6 != '00000000'
        THEN TRY_CAST(strptime(column6, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_exclusao_mei, 
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='simples') }}
