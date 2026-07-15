-- {{ ref('stg_estabelecimentos') }} {{ ref('dim_empresas') }} {{ ref('dim_tipos_estabelecimentos') }} {{ ref('dim_situacoes_cadastrais') }} {{ ref('dim_situacoes_cadastrais_motivos') }} {{ ref('dim_situacoes_cadastrais_motivos') }} {{ ref('dim_situacoes_especiais') }} {{ ref('dim_ufs') }} {{ ref('dim_municipios') }} {{ ref('dim_paises') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_estabelecimentos')
    ]
) }}

SELECT        
    es.sk_id,
    em.sk_id AS sk_empresas,
    te.sk_id AS sk_tipos_estabelecimentos,
    sc.sk_id AS sk_situacoes_cadastrais,
    scm.sk_id AS sk_situacoes_cadastrais_motivos,
    se.sk_id AS sk_situacoes_especiais,
    u.sk_id AS sk_ufs,
    m.sk_id AS sk_municipios,
    me.sk_id AS sk_municipios_exterior,
    p.sk_id AS sk_paises,
    tes.sk_id AS sk_data_inicio_atividade,
    COALESCE(tsc.sk_id,-1) AS sk_data_situacoes_cadastrais,
    COALESCE(tse.sk_id,-1) AS sk_data_situacoes_especiais,
    CAST(CASE WHEN es.identificador_matriz_filial = 1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_matrizes,
    CAST(CASE WHEN es.identificador_matriz_filial = 2 THEN 1 ELSE 0 END AS TINYINT) AS qtd_filiais,    
    CAST(CASE WHEN es.situacao_cadastral = 1 THEN 1 ELSE 0 END AS TINYINT) AS qtd_nulas,
    CAST(CASE WHEN es.situacao_cadastral = 2 THEN 1 ELSE 0 END AS TINYINT) AS qtd_ativas,
    CAST(CASE WHEN es.situacao_cadastral = 3 THEN 1 ELSE 0 END AS TINYINT) AS qtd_suspensas,
    CAST(CASE WHEN es.situacao_cadastral = 4 THEN 1 ELSE 0 END AS TINYINT) AS qtd_inaptas,
    CAST(CASE WHEN es.situacao_cadastral = 8 THEN 1 ELSE 0 END AS TINYINT) AS qtd_baixadas
FROM {{ ref('stg_estabelecimentos') }} es
JOIN {{ ref('dim_empresas') }} em ON em.cnpj_basico = es.cnpj_basico
JOIN {{ ref('dim_tipos_estabelecimentos') }} te ON te.codigo = es.identificador_matriz_filial
JOIN {{ ref('dim_situacoes_cadastrais') }} sc ON sc.codigo = es.situacao_cadastral
JOIN {{ ref('dim_situacoes_cadastrais_motivos') }} scm ON scm.codigo = es.motivo_situacao_cadastral
JOIN {{ ref('dim_situacoes_especiais') }} se ON se.codigo = es.situacao_especial
JOIN {{ ref('dim_ufs')}} u ON u.codigo = es.uf
JOIN {{ ref('dim_municipios')}} m ON m.codigo = es.municipio
JOIN {{ ref('dim_municipios_exterior')}} me ON me.codigo = es.cidade_exterior
JOIN {{ ref('dim_paises')}} p ON p.codigo = es.pais
JOIN {{ ref('dim_tempo_inicio_atividade') }} tes ON tes.data_referencia = es.data_inicio_atividade
LEFT JOIN {{ ref('dim_tempo_situacao_cadastral') }} tsc ON tsc.data_referencia = es.data_situacao_cadastral
LEFT JOIN {{ ref('dim_tempo_situacao_especial') }} tse ON tse.data_referencia = es.data_situacao_especial