{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='municipios_exterior')
    ]
) }}

WITH nome_cidade_exterior_distintas AS (
    SELECT DISTINCT column08 AS nome_cidade_exterior 
    FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }}
    WHERE column08 IS NOT NULL
),

nome_cidade_exterior_codificada AS (
    SELECT        
        ROW_NUMBER() OVER (ORDER BY nome_cidade_exterior) AS codigo,
        nome_cidade_exterior AS descricao
    FROM nome_cidade_exterior_distintas

    UNION ALL

    SELECT -1 AS codigo,'NÃO INFORMADO' AS descricao

    UNION ALL

    SELECT -2 AS codigo,'NÃO IDENTIFICADO' AS descricao
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,descricao) AS SMALLINT) AS sk_id,
    CAST(codigo AS SMALLINT) AS codigo,    
    descricao,    
    NOW() AS data_processamento
FROM nome_cidade_exterior_codificada