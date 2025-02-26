{% macro get_tipos_filial() %}
{{ 
    return(
      [
        ("inventarios", "'EA', 'SA', 'E9', 'S9'" ), 
        ("ajustes", "'EO', 'SO', 'E6', 'S6'"),
        ("avarias", "'EX', 'SX'"),
        ("vencidos", "'EY', 'SY'"), 
        ("seguros", "'E2', 'SQ'"), 
        ("doacoes", "'SZ'")
      ]
    ) 
}}
{% endmacro %}

{% macro get_tipos_cd() %}
{{ 
    return(
      [
        ("inventarios", "in('EA', 'SA') and kade_tx_nr_docto LIKE '%/%'"),
        ("ajustes", "in('EO', 'SO', 'E6', 'S6', 'E9', 'S9') or (sub_tipo in('EA', 'SA') and kade_tx_nr_docto NOT LIKE '%/%')"),
        ("avarias", "LIKE 'SX%'"),
        ("vencidos", "LIKE 'SY%'"),
        ("seguros", "in('E2', 'SQ')"), 
        ("doacoes", " = 'SZ'")
      ]
    ) 
}}
{% endmacro %}