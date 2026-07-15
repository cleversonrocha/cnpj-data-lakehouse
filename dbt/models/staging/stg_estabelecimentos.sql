-- {{ ref('stg_tipos_estabelecimentos') }} {{ ref('stg_situacoes_cadastrais') }} {{ ref('stg_situacoes_cadastrais_motivos') }} {{ ref('stg_municipios_exterior') }} {{ ref('stg_paises') }} {{ ref('stg_cnaes') }} {{ ref('stg_ufs') }} {{ ref('stg_municipios') }}  {{ ref('stg_situacoes_especiais') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='estabelecimentos')
    ]
) }}

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY column00,column01,column02) AS INTEGER) AS sk_id,    
    column00 AS cnpj_basico,
    column01 AS cnpj_ordem,
    column02 AS cnpj_dv,        
    CASE
        WHEN column03 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
        WHEN te.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO        
        ELSE CAST(te.codigo AS TINYINT)
    END AS identificador_matriz_filial,
    column04 AS nome_fantasia,
    CASE
        WHEN column05 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
        WHEN sc.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO        
        ELSE CAST(sc.codigo AS TINYINT)
    END AS situacao_cadastral,
    CASE 
        WHEN LENGTH(TRIM(column06)) = 8 AND column06 != '00000000' THEN TRY_CAST(strptime(column06, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_situacao_cadastral,        
    CASE
        WHEN column07 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
        WHEN scm.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO        
        ELSE CAST(scm.codigo AS TINYINT)
    END AS motivo_situacao_cadastral,
    CASE
        WHEN column08 IS NULL THEN CAST(-1 AS SMALLINT) --NÃO INFORMADO
        WHEN me.codigo IS NULL THEN CAST(-2 AS SMALLINT) --NÃO IDENTIFICADO
        ELSE CAST(me.codigo AS SMALLINT)
    END AS cidade_exterior,
    CASE
        --Se identificou o país na tabela países
        WHEN p.codigo IS NOT NULL THEN CAST(column09 AS SMALLINT)
        --Se não identificou o país e a cidade não é EXTERIOR (9707)
        WHEN p.codigo IS NULL AND mu.codigo != '9707' THEN CAST(105 AS SMALLINT) --BRASIL
        --Se não identificou o país e esta como EXTERIOR
        WHEN p.codigo IS NULL AND mu.codigo == '9707' THEN CAST(-2 AS SMALLINT) --NÃO IDENTIFICADO
        ELSE -1 --NÃO INFORMADO
    END AS pais,    
    CASE 
        WHEN LENGTH(TRIM(column10)) = 8 AND column10 != '00000000' THEN TRY_CAST(strptime(column10, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_inicio_atividade,
    CASE
        WHEN column11 IS NULL THEN '-1' --NÃO INFORMADO
        WHEN c.codigo IS NULL THEN '-2' --NÃO IDENTIFICADO
        ELSE c.codigo
    END AS cnae_fiscal_principal,
    array_to_string(
        list_distinct(
            list_transform(string_split(column12, ','), x -> TRIM(x))
        ), ','
    ) AS cnae_fiscal_secundaria, 
    column13 AS tipo_logradouro,
    column14 AS logradouro,    
    column15 AS numero,    
    column16 AS complemento,
    column17 AS bairro,
    column18 AS cep,
    CASE
        WHEN column19 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
        WHEN u.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO
        ELSE CAST(u.codigo AS TINYINT)
    END AS uf,
    CASE
        WHEN column20 IS NULL THEN CAST(-1 AS SMALLINT) --NÃO INFORMADO
        WHEN mu.codigo IS NULL THEN CAST(-2 AS SMALLINT) --NÃO IDENTIFICADO
        ELSE CAST(mu.codigo AS SMALLINT)
    END AS municipio,
    column21 AS ddd_1,
    column22 AS telefone_1,
    column23 AS ddd_2,
    column24 AS telefone_2,
    column25 AS ddd_fax,
    column26 AS fax,
    column27 AS email,
    CASE
        WHEN column28 IS NULL THEN CAST(-1 AS TINYINT) --NÃO INFORMADO
        WHEN se.codigo IS NULL THEN CAST(-2 AS TINYINT) --NÃO IDENTIFICADO
        ELSE CAST(se.codigo AS TINYINT) 
    END AS situacao_especial,
    CASE 
        WHEN LENGTH(TRIM(column29)) = 8 AND column29 != '00000000'
        THEN TRY_CAST(strptime(column29, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_situacao_especial,        
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }} e
LEFT JOIN {{ ref('stg_tipos_estabelecimentos') }} te ON te.codigo = e.column03
LEFT JOIN {{ ref('stg_situacoes_cadastrais') }} sc ON sc.codigo = e.column05
LEFT JOIN {{ ref('stg_situacoes_cadastrais_motivos') }} scm ON scm.codigo = e.column07
LEFT JOIN {{ ref('stg_municipios_exterior') }} me ON me.descricao = e.column08
LEFT JOIN {{ ref('stg_paises') }} p ON p.codigo = e.column09
LEFT JOIN {{ ref('stg_cnaes') }} c ON c.codigo = e.column11
LEFT JOIN {{ ref('stg_ufs') }} u ON u.descricao = e.column19
LEFT JOIN {{ ref('stg_municipios') }} mu ON mu.codigo = e.column20
LEFT JOIN {{ ref('stg_situacoes_especiais') }} se ON se.descricao = e.column28