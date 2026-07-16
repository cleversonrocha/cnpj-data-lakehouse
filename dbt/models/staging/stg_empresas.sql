-- {{ ref('stg_naturezas') }} {{ ref('stg_qualificacoes') }} {{ ref('stg_portes_empresas') }} {{ ref('stg_entes_federativos') }} {{ ref('stg_simples') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='empresas')
    ]
) }}

WITH empresas_qualificadas AS (
    SELECT
        column0,
        column1,
        column2,
        column3,
        column4,
        column5,
        column6,
        CAST(    
            ROW_NUMBER() OVER (
                PARTITION BY column0
                ORDER BY
                    (CASE WHEN column1 IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN column2 IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN column3 IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN column4 IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN column5 IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN column6 IS NOT NULL THEN 1 ELSE 0 END) DESC,
                    column1 DESC NULLS LAST -- desempate determinístico se completude empatar
            ) AS INTEGER
        ) AS rn
    FROM {{ get_s3_path(base_path='silver/raw', file_name='empresas') }} em
    QUALIFY rn = 1
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY column0) AS INTEGER) AS sk_id,
    column0 AS cnpj_basico,        
    column1 AS razao_social,
    CASE
        WHEN column2 IS NULL THEN CAST(-1 AS SMALLINT) --NÃO INFORMADO
        WHEN nj.codigo IS NULL THEN CAST(-2 AS SMALLINT) --NÃO IDENTIFICADO
        ELSE CAST(nj.codigo AS SMALLINT)
    END AS natureza_juridica,
    CASE
        WHEN column3 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
        WHEN q.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO
        ELSE CAST(q.codigo AS TINYINT)
    END AS qualificacao_responsavel,    
    TRY_CAST(REPLACE(column4, ',', '.') AS DECIMAL(15, 2)) AS capital_social,
    CASE
        WHEN column5 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
        WHEN pe.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO
        ELSE CAST(pe.codigo AS TINYINT)
    END AS porte_empresa,    
    CASE
        WHEN column6 IS NULL THEN CAST(-1 AS SMALLINT) --NÃO INFORMADO
        WHEN ef.codigo IS NULL THEN CAST(-2 AS SMALLINT) --NÃO IDENTIFICADO
        ELSE CAST(ef.codigo AS SMALLINT)
    END AS ente_federativo_responsavel,    
    CASE        
        WHEN s.opcao_mei = 'S' AND s.data_exclusao_mei IS NULL THEN CAST(-1 AS TINYINT) --SIM
        ELSE CAST(-2 AS TINYINT) --NÃO
    END AS is_mei,
    NOW() AS data_processamento
FROM empresas_qualificadas em
LEFT JOIN {{ ref('stg_naturezas') }} nj ON nj.codigo = em.column2
LEFT JOIN {{ ref('stg_qualificacoes') }} q ON q.codigo = em.column3
LEFT JOIN {{ ref('stg_portes_empresas') }} pe ON pe.codigo = em.column5
LEFT JOIN {{ ref('stg_entes_federativos') }} ef ON ef.descricao = em.column6
LEFT JOIN {{ ref('stg_simples') }} s ON s.cnpj_basico = em.column0