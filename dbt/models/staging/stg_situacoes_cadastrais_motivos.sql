{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='situacoes_cadastrais_motivos')
    ]
) }}

WITH motivos AS (
    SELECT 
        column0 AS codigo,
        column1 AS descricao        
    FROM {{ get_s3_path(base_path='silver/raw', file_name='motivos') }}

    UNION ALL

    SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

    UNION ALL

    SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,descricao) AS TINYINT) AS sk_id,
    CAST(codigo AS TINYINT) AS codigo,
    descricao,
    NOW() AS data_processamento
FROM motivos