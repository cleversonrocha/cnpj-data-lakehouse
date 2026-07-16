{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/raw', file_name = 'int_empresas_nao_qualificadas')
    ]
) }}

WITH empresas AS (
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
    QUALIFY rn > 1
)

SELECT * FROM empresas