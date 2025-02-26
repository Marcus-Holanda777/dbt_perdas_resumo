{%- macro cnpj_normalize(colname) -%}
  LPAD(REGEXP_REPLACE(TRIM({{ colname }}), '[^0-9a-zA-Z]', ''), 15, '0')
{%- endmacro -%}