{%- macro servicos(alias="pm", column="capr_cd_categoria") -%}
 AND {{ alias }}.{{ column }} NOT LIKE '3%'
 AND {{ alias }}.{{ column }} NOT LIKE '1.101.009%'
 AND {{ alias }}.{{ column }} NOT LIKE '1.102.009%'
 AND {{ alias }}.{{ column }} NOT LIKE '2.504.001%'
{% endmacro -%}