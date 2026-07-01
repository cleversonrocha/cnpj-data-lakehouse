{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='estabelecimentos')
    ]
) }}

WITH estabelecimentos AS (   
    SELECT
        CAST(ROW_NUMBER() OVER() AS INTEGER) AS sk_id,    
        CAST(column00 AS VARCHAR) AS cnpj_basico,
        CAST(column01 AS VARCHAR) AS cnpj_ordem,
        CAST(column02 AS VARCHAR) AS cnpj_dv,        
        CAST(column03 AS INTEGER) AS identificador_matriz_filial,
        CAST(column04 AS VARCHAR) AS nome_fantasia,
        CAST(column05 AS INTEGER) AS situacao_cadastral,    
        CASE 
            WHEN LENGTH(TRIM(column06)) = 8 AND column06 != '00000000'
            THEN TRY_CAST(strptime(column06, '%Y%m%d') AS DATE)
            ELSE NULL
        END AS data_situacao_cadastral,
        CAST(column07 AS INTEGER) AS motivo_situacao_cadastral,
        CAST(column08 AS VARCHAR) AS nome_cidade_exterior,
        CASE            
            --Se achou o municipio e for diferente de EXTERIOR e o estado for diferente de EX, então o país será 105 (Brasil)
            WHEN m.column0 IS NOT NULL AND m.column0 != '9707' THEN 105 --BRASIL
            --Se achou o pais na tabela de paises retorna o mesmo caso contrario define como 999 (NAO DECLARADOS)            
            ELSE COALESCE(CAST(p.column0 AS INTEGER),999) -- 999 (NAO DECLARADOS)
        END AS pais,
        null AS desc_pais,
        CASE 
            WHEN LENGTH(TRIM(column10)) = 8 AND column10 != '00000000'
            THEN TRY_CAST(strptime(column10, '%Y%m%d') AS DATE)
            ELSE NULL
        END AS data_inicio_atividade,
        CAST(column11 AS VARCHAR) AS cnae_fiscal_principal,    
        array_to_string(
            list_distinct(
                list_transform(string_split(column12, ','), x -> TRIM(x))
            ), ','
        ) AS cnae_fiscal_secundaria, 
        CAST(column13 AS VARCHAR) AS tipo_logradouro,
        CAST(column14 AS VARCHAR) AS logradouro,    
        CAST(column15 AS VARCHAR) AS numero,    
        CAST(column16 AS VARCHAR) AS complemento,
        CAST(column17 AS VARCHAR) AS bairro,
        CAST(column18 AS VARCHAR) AS cep,
        CAST(column19 AS VARCHAR) AS uf,        
        CAST(column20 AS INTEGER) AS municipio,
        null AS desc_municipio,
        CAST(column21 AS VARCHAR) AS ddd_1,
        CAST(column22 AS VARCHAR) AS telefone_1,
        CAST(column23 AS VARCHAR) AS ddd_2,
        CAST(column24 AS VARCHAR) AS telefone_2,
        CAST(column25 AS VARCHAR) AS ddd_fax,
        CAST(column26 AS VARCHAR) AS fax,
        CAST(column27 AS VARCHAR) AS email,      
        CAST(column28 AS VARCHAR) AS situacao_especial,    
        CASE 
            WHEN LENGTH(TRIM(column29)) = 8 AND column29 != '00000000'
            THEN TRY_CAST(strptime(column29, '%Y%m%d') AS DATE)
            ELSE NULL
        END AS data_situacao_especial,        
        NOW() AS data_processamento
    FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }} e
    LEFT JOIN read_parquet('s3://silver/raw/2026_06/paises.parquet') p ON p.column0 = e.column09
    LEFT JOIN {{ get_s3_path(base_path='silver/raw', file_name='municipios') }} m ON m.column0 = e.column20
)

SELECT sk_id,
       cnpj_basico, 
       cnpj_ordem,
       cnpj_dv,        
       identificador_matriz_filial,
       nome_fantasia,
       data_inicio_atividade,    
       situacao_cadastral,    
       data_situacao_cadastral,    
       motivo_situacao_cadastral,
       data_situacao_especial,
       situacao_especial,
       pais,
       p.column1 AS desc_pais,
       uf,
       municipio,
       COALESCE(nome_cidade_exterior,m.column1) AS desc_municipio,
       tipo_logradouro,
       logradouro,    
       numero,    
       complemento,
       bairro,
       cep,
       ddd_1,
       telefone_1,
       ddd_2,
       telefone_2,
       ddd_fax,
       fax,
       email,      
       cnae_fiscal_principal,    
       cnae_fiscal_secundaria,     
       data_processamento
FROM estabelecimentos es
LEFT JOIN read_parquet('s3://silver/raw/2026_06/paises.parquet') p ON p.column0 = es.pais
LEFT JOIN {{ get_s3_path(base_path='silver/raw', file_name='municipios') }} m ON m.column0 = es.municipio