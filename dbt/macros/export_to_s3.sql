{% macro export_to_s3(bucket_path, file_name) %}    

    {% set ano_mes = get_ano_mes() %}    
    
    {% set query %}
        COPY {{ this }} TO 's3://{{ bucket_path }}/{{ ano_mes }}/{{ file_name }}.parquet' (FORMAT PARQUET)
    {% endset %}

    {{ return(query) }}

{% endmacro %}