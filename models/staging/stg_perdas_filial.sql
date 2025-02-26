{% set tipos = get_tipos_filial() %}
{% set limit_ref = modules.datetime.datetime.strptime(var("limit_date"), "%Y-%m-%d") %}
{% set start_parser = modules.datetime.datetime.strptime(var("start", "1999-01-01"), "%Y-%m-%d") %}
{% set end_parser = modules.datetime.datetime.strptime(var("end", "1999-01-01"), "%Y-%m-%d") %}

WITH start_perdas AS (
    SELECT
        kp.kafi_cd_filial,
        kp.kafi_cd_produto,
        kp.kafi_vl_cmpcsicms,
        kp.kafi_dh_ocorrreal,
        UPPER(TRIM(kp.kafi_tp_mov)) AS kafi_tp_mov,
        {{ qtd_mov() }} AS kafi_qt_mov
    FROM {{ source('prevencao-perdas', 'kardex_perdas') }} AS kp
    INNER JOIN
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_produto_mestre') }} AS pm
        ON kp.kafi_cd_produto = pm.prme_cd_produto
    WHERE
        kp.kafi_tx_nr_docto NOT LIKE '%INVT.INI.EF%'
        {{ servicos() }}
        {% if start_parser > limit_ref %}
        AND kp.kafi_fl_tipoperda is null
        {% endif %}
        AND kp.kafi_dh_ocorrreal
        BETWEEN TIMESTAMP '{{ start_parser.strftime("%Y-%m-%d 00:00:00.000") }}'
        AND TIMESTAMP '{{ end_parser.strftime("%Y-%m-%d 23:59:59.999") }}'
),

add_tipo_perdas AS (
    SELECT
        kafi_cd_filial AS filial,
        kafi_cd_produto AS cod_prod,
        DATE_TRUNC('month', kafi_dh_ocorrreal) AS periodo,
        {%- for coluna, eventos in tipos %}
            SUM(kafi_qt_mov) FILTER (
                WHERE kafi_tp_mov IN ({{ eventos }})
            ) AS qtd_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        {%- for coluna, eventos in tipos %}
            SUM(kafi_qt_mov * kafi_vl_cmpcsicms) FILTER (
                WHERE kafi_tp_mov IN ({{ eventos }})
            ) AS vl_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %}
    FROM start_perdas
    GROUP BY 1, 2, 3
),

coalesce_tipos AS (
    SELECT
        filial,
        cod_prod,
        'filial' AS origem,
        periodo,
        {%- for coluna, eventos in tipos %}
            COALESCE(qtd_{{ coluna }}, 0) AS qtd_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        {%- for coluna, eventos in tipos %}
            COALESCE(vl_{{ coluna }}, 0) AS vl_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %}
    FROM add_tipo_perdas
),

final AS (
    SELECT
        filial,
        cod_prod,
        origem,
        {%- for coluna, eventos in tipos %}
            qtd_{{ coluna }}
            {%- if coluna ==  "ajustes" %}
                ,
                qtd_inventarios + qtd_{{ coluna }} AS qtd_perda_desconhecida
            {%- elif coluna ==  "vencidos" %}
                ,
                qtd_avarias + qtd_{{ coluna }} AS qtd_perda_conhecida
            {%- endif -%}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        {%- for coluna, eventos in tipos %}
            vl_{{ coluna }}
            {%- if coluna ==  "ajustes" %}
                ,
                vl_inventarios + vl_{{ coluna }} AS vl_perda_desconhecida
            {%- elif coluna ==  "vencidos" %}
                ,
                vl_avarias + vl_{{ coluna }} AS vl_perda_conhecida
            {%- endif -%}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        periodo
    FROM coalesce_tipos
)

SELECT * FROM final
