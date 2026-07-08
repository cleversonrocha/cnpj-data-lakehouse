{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_quadro_societario')
    ]
) }}

SELECT
    s.sk_id,
    s.cnpj_basico,
    CASE s.identificador    	
        WHEN 1 THEN 'PESSOA FÍSICA'
        WHEN 2 THEN 'PESSOA JURÍDICA'
        WHEN 3 THEN 'ESTRANGEIRO'        
    END AS pessoa,
    nome_razao_social,
    cpf_cnpj,        
    qs.descricao AS qualificacao_socio,    
    p.descricao AS pais_socio,
    representante_legal,
    nome_do_representante,
    qr.descricao AS qualificacao_representante,
    CASE s.faixa_etaria_socio
        WHEN 1 THEN '0 a 12 anos' 
        WHEN 2 THEN '13 a 20 anos' 
        WHEN 3 THEN '21 a 30 anos' 
        WHEN 4 THEN '31 a 40 anos' 
        WHEN 5 THEN '41 a 50 anos' 
        WHEN 6 THEN '51 a 60 anos' 
        WHEN 7 THEN '61 a 70 anos' 
        WHEN 8 THEN '71 a 80 anos' 
        WHEN 9 THEN 'maiores de 80 anos' 
        WHEN 0 THEN 'não se aplica'        
    END as faixa_etaria_socio    
FROM {{ ref('stg_socios') }} s
JOIN {{ ref('stg_qualificacoes') }} qs ON qs.codigo = s.qualificacao 
JOIN {{ ref('stg_qualificacoes') }} qr ON qr.codigo = s.qualificacao_representante
LEFT JOIN {{ ref('stg_paises') }} p ON p.codigo = s.pais