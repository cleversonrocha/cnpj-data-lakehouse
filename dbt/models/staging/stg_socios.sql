-- {{ ref('stg_tipos_pessoas') }} {{ ref('stg_qualificacoes') }} {{ ref('stg_paises') }} {{ ref('stg_qualificacoes') }} {{ ref('stg_faixas_etarias') }} ← comentário que força dependência

{{ config(
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='socios')
    ]
) }}

WITH socios AS (
    SELECT
        DISTINCT --TEM SOCIOS DUPLICADOS EX.: CNPJ BASICO (02872412) - PODE SER UM MESMO SÓCIO QUE DESEMPENHA ATIVIDADES COMO ADMINISTRADOR E SÓCIO
        column00 AS cnpj_basico,        
        CASE
            WHEN column01 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
            WHEN tp.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO
            ELSE CAST(tp.codigo AS TINYINT)
        END AS identificador,
        column02 AS nome_razao_social,
        column03 AS cpf_cnpj,
        CASE
            WHEN column04 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
            WHEN q.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO
            ELSE CAST(q.codigo AS TINYINT)
        END AS qualificacao,
        CASE 
            WHEN LENGTH(TRIM(column05)) = 8 AND column05 != '00000000' THEN TRY_CAST(strptime(column05, '%Y%m%d') AS DATE)
            ELSE NULL
        END AS data_entrada_sociedade,
        CASE
            WHEN column06 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
            WHEN p.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO
            ELSE CAST(p.codigo AS SMALLINT)
        END AS pais,
        column07 AS representante_legal,
        column08 AS nome_do_representante,
        CASE
            WHEN column09 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
            WHEN qr.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO
            ELSE CAST(qr.codigo AS TINYINT)
        END AS qualificacao_representante,
        CASE
            WHEN column10 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
            WHEN fe.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO
            ELSE CAST(fe.codigo AS TINYINT)
        END AS faixa_etaria_socio
    FROM {{ get_s3_path(base_path='silver/raw', file_name='socios') }} s
    LEFT JOIN {{ ref('stg_tipos_pessoas')}} tp ON tp.codigo = s.column01
    LEFT JOIN {{ ref('stg_qualificacoes')}} q ON q.codigo = s.column04
    LEFT JOIN {{ ref('stg_paises')}} p ON p.codigo = s.column06
    LEFT JOIN {{ ref('stg_qualificacoes')}} qr ON qr.codigo = s.column09
    LEFT JOIN {{ ref('stg_faixas_etarias')}} fe ON fe.codigo = s.column10
)

SELECT
    CAST(ROW_NUMBER() OVER() AS INTEGER) AS sk_id,
    cnpj_basico,
    identificador,
    nome_razao_social,
    cpf_cnpj,
    qualificacao,
    data_entrada_sociedade,
    pais,
    representante_legal,
    nome_do_representante,
    qualificacao_representante,
    faixa_etaria_socio,    
    NOW() AS data_processamento
FROM socios