{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_socios')
    ]
) }}

SELECT    
    s.sk_id AS sk_socio,
    e.sk_id AS sk_empresa,    
    COALESCE(CAST(STRFTIME('%Y%m%d', s.data_entrada_sociedade) AS INTEGER),-1) AS sk_tempo_entrada_sociedade,
    CAST(CASE WHEN s.identificador = 1 THEN 1 ELSE 0 END AS TINYINT) AS is_pf,
    CAST(CASE WHEN s.identificador = 2 THEN 1 ELSE 0 END AS TINYINT) AS is_pj,
    CAST(CASE WHEN s.identificador = 3 THEN 1 ELSE 0 END AS TINYINT) AS is_estrangeiro,
    NOW() AS data_processamento
FROM {{ ref('dim_socios') }} s
JOIN {{ ref('dim_empresas')}} e ON e.cnpj_basico = s.cnpj_basico