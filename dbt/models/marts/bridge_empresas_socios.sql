-- {{ ref('stg_empresas') }} {{ ref('stg_socios') }} {{ ref('dim_tipos_pessoas') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/bridge', file_name='bridge_empresas_socios')
    ]
) }}

SELECT
    em.sk_id AS sk_empresa_id,
    ss.sk_id AS sk_socio_id,
    tp.sk_id AS sk_tipos_pessoas,
    COALESCE(tes.sk_id,-1) AS sk_data_entrada_sociedade
FROM {{ ref('stg_empresas') }} em
JOIN {{ ref('stg_socios') }} ss ON ss.cnpj_basico = em.cnpj_basico
JOIN {{ ref('dim_tipos_pessoas') }} tp ON tp.codigo = ss.identificador
LEFT JOIN {{ ref('dim_tempo_entrada_sociedade') }} tes ON tes.data_referencia = ss.data_entrada_sociedade