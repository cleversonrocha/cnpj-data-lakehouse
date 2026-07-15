-- {{ ref('stg_socios') }} {{ ref('dim_tipos_pessoas') }} {{ ref('dim_qualificacoes') }} {{ ref('dim_faixas_etarias') }} {{ ref('dim_paises') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/fact', file_name='fact_socios')
    ]
) }}

SELECT
    s.sk_id,
    tp.sk_id AS sk_tipo_pessoa,
    qs.sk_id AS sk_qualificacao_socio,
    fe.sk_id AS sk_faixa_etaria_socio,
    p.sk_id AS sk_pais_socio,
    qr.sk_id AS sk_qualificacao_representante,
    CAST(CASE WHEN s.identificador = 1 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_socios_pf,
    CAST(CASE WHEN s.identificador = 2 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_socios_pj,
    CAST(CASE WHEN s.identificador = 3 THEN 1 ELSE 0 END AS SMALLINT) AS qtd_socios_estrangeiro    
FROM {{ ref('stg_socios') }} s
JOIN {{ ref('dim_tipos_pessoas') }} tp ON tp.codigo = s.identificador
JOIN {{ ref('dim_qualificacoes') }} qs ON qs.codigo = s.qualificacao
JOIN {{ ref('dim_qualificacoes') }} qr ON qr.codigo = s.qualificacao_representante
JOIN {{ ref('dim_faixas_etarias') }} fe ON fe.codigo = s.faixa_etaria_socio
JOIN {{ ref('dim_paises') }} p ON p.codigo = s.pais