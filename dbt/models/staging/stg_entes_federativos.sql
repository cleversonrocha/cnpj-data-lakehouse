{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='entes_federativos')
    ]
) }}

WITH entes_federativos AS (
    SELECT DISTINCT column6 AS ente_federativo_responsavel  
    FROM {{ get_s3_path(base_path='silver/raw', file_name='empresas') }}
    WHERE column6 IS NOT NULL   
),

entes_federativos_codificada AS (
    SELECT        
        ROW_NUMBER() OVER (ORDER BY ente_federativo_responsavel) AS codigo,
        ente_federativo_responsavel AS descricao
    FROM entes_federativos

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
FROM entes_federativos_codificada