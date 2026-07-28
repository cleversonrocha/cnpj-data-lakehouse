{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_empresas')
    ]
) }}

SELECT
    e.sk_id AS sk_empresa,
    e.capital_social,
    epe.qtd_estabelecimentos,
    epe.qtd_matrizes,
    epe.qtd_filiais,
    epe.qtd_situacao_nulas,
    epe.qtd_situacao_ativas,
    epe.qtd_situacao_suspensas,
    epe.qtd_situacao_inaptas,
    epe.qtd_situacao_baixadas,
    spe.qtd_socios,
    spe.qtd_socios_pf,
    spe.qtd_socios_pj,
    spe.qtd_socios_estrangeiro,
    CAST(CASE WHEN pe.codigo = 1 THEN 1 ELSE 0 END AS TINYINT) AS is_me,
    CAST(CASE WHEN pe.codigo = 3 THEN 1 ELSE 0 END AS TINYINT) AS is_pp,
    CAST(CASE WHEN pe.codigo = 5 THEN 1 ELSE 0 END AS TINYINT) AS is_demais,    
    CAST(CASE WHEN s.opcao_simples = 'S' THEN 1 ELSE 0 END AS TINYINT) AS is_simples,    
    CAST(CASE WHEN s.opcao_mei = 'S' THEN 1 ELSE 0 END AS TINYINT) AS is_mei,
    NOW() AS data_processamento
FROM {{ ref('dim_empresas') }} e
JOIN {{ ref('int_estabelecimentos_por_empresa') }} epe ON epe.cnpj_basico = e.cnpj_basico
LEFT JOIN {{ ref('int_socios_por_empresa') }} spe ON spe.cnpj_basico = e.cnpj_basico
LEFT JOIN {{ ref('int_portes_empresas') }} pe ON pe.codigo = e.porte_empresa
LEFT JOIN {{ ref('int_simples') }} s ON s.cnpj_basico = e.cnpj_basico