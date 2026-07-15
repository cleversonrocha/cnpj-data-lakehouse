{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='ufs')
    ]
) }}

WITH uf_distintas AS (
    SELECT DISTINCT column19 as uf 
    FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }}
    WHERE column19 IS NOT NULL    
),

uf_codificada AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY uf) AS codigo,
        uf AS sigla,
        CASE
            WHEN uf = 'AC' THEN 'Acre'
            WHEN uf = 'AP' THEN 'Amapá'
            WHEN uf = 'AM' THEN 'Amazonas'
            WHEN uf = 'PA' THEN 'Pará'
            WHEN uf = 'RO' THEN 'Rondônia'
            WHEN uf = 'RR' THEN 'Roraima'
            WHEN uf = 'TO' THEN 'Tocantins'
            WHEN uf = 'AL' THEN 'Alagoas'
            WHEN uf = 'BA' THEN 'Bahia'
            WHEN uf = 'CE' THEN 'Ceará'
            WHEN uf = 'MA' THEN 'Maranhão'
            WHEN uf = 'PB' THEN 'Paraíba'
            WHEN uf = 'PE' THEN 'Pernambuco'
            WHEN uf = 'PI' THEN 'Piauí'
            WHEN uf = 'RN' THEN 'Rio Grande do Norte'
            WHEN uf = 'SE' THEN 'Sergipe'
            WHEN uf = 'DF' THEN 'Distrito Federal'
            WHEN uf = 'GO' THEN 'Goiás'
            WHEN uf = 'MT' THEN 'Mato Grosso'
            WHEN uf = 'MS' THEN 'Mato Grosso do Sul'
            WHEN uf = 'ES' THEN 'Espírito Santo'
            WHEN uf = 'MG' THEN 'Minas Gerais'
            WHEN uf = 'RJ' THEN 'Rio de Janeiro'
            WHEN uf = 'SP' THEN 'São Paulo'
            WHEN uf = 'PR' THEN 'Paraná'
            WHEN uf = 'RS' THEN 'Rio Grande do Sul'
            WHEN uf = 'SC' THEN 'Santa Catarina'
            WHEN uf = 'EX' THEN 'Exterior'            
        END AS descricao,
        CASE 
            WHEN uf = 'AC' THEN 'NORTE'
            WHEN uf = 'AP' THEN 'NORTE'
            WHEN uf = 'AM' THEN 'NORTE'
            WHEN uf = 'PA' THEN 'NORTE'
            WHEN uf = 'RO' THEN 'NORTE'
            WHEN uf = 'RR' THEN 'NORTE'
            WHEN uf = 'TO' THEN 'NORTE'
            WHEN uf = 'AL' THEN 'NORDESTE'
            WHEN uf = 'BA' THEN 'NORDESTE'
            WHEN uf = 'CE' THEN 'NORDESTE'
            WHEN uf = 'MA' THEN 'NORDESTE'
            WHEN uf = 'PB' THEN 'NORDESTE'
            WHEN uf = 'PE' THEN 'NORDESTE'
            WHEN uf = 'PI' THEN 'NORDESTE'
            WHEN uf = 'RN' THEN 'NORDESTE'
            WHEN uf = 'SE' THEN 'NORDESTE'
            WHEN uf = 'DF' THEN 'CENTRO-OESTE'
            WHEN uf = 'GO' THEN 'CENTRO-OESTE'
            WHEN uf = 'MT' THEN 'CENTRO-OESTE'
            WHEN uf = 'MS' THEN 'CENTRO-OESTE'
            WHEN uf = 'ES' THEN 'SUDESTE'
            WHEN uf = 'MG' THEN 'SUDESTE'
            WHEN uf = 'RJ' THEN 'SUDESTE'
            WHEN uf = 'SP' THEN 'SUDESTE'
            WHEN uf = 'PR' THEN 'SUL'
            WHEN uf = 'RS' THEN 'SUL'
            WHEN uf = 'SC' THEN 'SUL'
            WHEN uf = 'EX' THEN 'EXTERIOR'            
        END AS regiao
    FROM uf_distintas

    UNION ALL

    SELECT -1 AS codigo, 'NÃO INF.' AS sigla, 'NÃO INFORMADO' AS descricao, 'NÃO INFORMADO' AS regiao

    UNION ALL

    SELECT -2 AS codigo, 'NÃO IDENT.' AS sigla, 'NÃO INDENTIFICADO' AS descricao, 'NÃO INDENTIFICADO' AS regiao
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,sigla) AS TINYINT) AS sk_id,
    CAST(codigo AS TINYINT) AS codigo,
    sigla,
    descricao,
    regiao,
    NOW() AS data_processamento
FROM uf_codificada