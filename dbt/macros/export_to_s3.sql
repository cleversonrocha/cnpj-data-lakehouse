{% macro export_to_s3(bucket_path, file_name) %}    

    {% if var('ano_mes', '') != '' %}    
        {% set ano_mes = var('ano_mes') %}
    {% else %}    
        {% set ano_mes = modules.datetime.datetime.now().strftime('%Y_%m') %}
    {% endif %}       
    
    {% set query %}
        COPY {{ this }} TO 's3://{{ bucket_path }}/{{ ano_mes }}/{{ file_name }}.parquet' (FORMAT PARQUET)
    {% endset %}

    {{ return(query) }}

{% endmacro %}