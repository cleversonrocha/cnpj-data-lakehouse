{% macro export_to_s3(bucket_path, file_name) %}

    {% set data_atual = modules.datetime.datetime.now().strftime('%Y_%m') %}        
    
    {% set query %}
        COPY {{ this }} TO 's3://{{ bucket_path }}/{{ data_atual }}/{{ file_name }}.parquet' (FORMAT PARQUET)
    {% endset %}

    {{ return(query) }}

{% endmacro %}