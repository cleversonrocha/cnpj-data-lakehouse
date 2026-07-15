-- {{ ref('stg_estabelecimentos') }} ← comentário que força dependência

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/dim', file_name='dim_estabelecimentos')
    ]
) }}

SELECT
    sk_id,
    cnpj_basico,    
    SUBSTR(cnpj_basico, 1, 2) || '.' || SUBSTR(cnpj_basico, 3, 3) || '.' || SUBSTR(cnpj_basico, 6, 3) || '/' || cnpj_ordem || '-' || cnpj_dv AS cnpj_completo,    
    nome_fantasia,
    tipo_logradouro || ' ' || logradouro AS logradouro,    
    numero,    
    complemento,
    bairro,
    CASE 
        WHEN cep IS NULL THEN cep
        ELSE SUBSTR(cep, 1, 5) || '-' || SUBSTR(cep, 6, 3)
    END AS cep,
    CASE 
        WHEN ddd_1 IS NULL OR telefone_1 IS NULL THEN NULL
        ELSE '(' || ddd_1 || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(telefone_1, '\D', '', 'g')) = 9            
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(telefone_1, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(telefone_1, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(telefone_1, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE telefone_1
            END
    END AS telefone_1,
    CASE 
        WHEN ddd_2 IS NULL OR telefone_2 IS NULL THEN NULL
        ELSE '(' || ddd_2 || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(telefone_2, '\D', '', 'g')) = 9            
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(telefone_2, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(telefone_2, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(telefone_2, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE telefone_2
            END
    END AS telefone_2,
    CASE 
        WHEN ddd_fax IS NULL OR fax IS NULL THEN NULL
        ELSE '(' || ddd_fax || ') ' || 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(fax, '\D', '', 'g')) = 9             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(fax, '\D', '', 'g'), '^(\d{5})(\d{4})$', '\1-\2')
                WHEN LENGTH(REGEXP_REPLACE(fax, '\D', '', 'g')) = 8             
                    THEN REGEXP_REPLACE(REGEXP_REPLACE(fax, '\D', '', 'g'), '^(\d{4})(\d{4})$', '\1-\2')
                ELSE fax
            END
    END AS fax,  
    email
FROM {{ ref('stg_estabelecimentos') }}