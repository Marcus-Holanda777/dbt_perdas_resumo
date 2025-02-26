{% set tipos = get_tipos_cd() %}
{% set limit_ref = modules.datetime.datetime.strptime(var("limit_date"), "%Y-%m-%d") %}
{% set start_parser = modules.datetime.datetime.strptime(var("start", "1999-01-01"), "%Y-%m-%d") %}
{% set end_parser = modules.datetime.datetime.strptime(var("end", "1999-01-01"), "%Y-%m-%d") %}

WITH deposito_filial AS (
    SELECT
        depo_cd_deposito,
        CAST(SUBSTR(TRIM(depo_tn_cnpj), 13, 4) AS int) AS filial
    FROM {{ source("prevencao-perdas", "cosmos_v14b_dbo_deposito") }}
),

start_perdas AS (
    SELECT
        df.filial,
        kp.kade_cd_produto,
        kp.kade_vl_cmpcsicms,
        kp.kade_dh_ocorr,
        TRIM(kp.kade_tx_nr_docto) AS kade_tx_nr_docto,
        UPPER(TRIM(kp.kade_tp_mov)) AS kade_tp_mov,
        TRIM(kp.sub_tipo) AS sub_tipo,
        {{ qtd_mov("kade") }} AS kade_qt_mov
    FROM {{ source('prevencao-perdas', 'kardex_perdas_cd') }} AS kp
    INNER JOIN
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_produto_mestre') }} AS pm
        ON kp.kade_cd_produto = pm.prme_cd_produto
    INNER JOIN
        deposito_filial AS df
        ON kp.kade_cd_deposito = df.depo_cd_deposito
    WHERE
        kp.kade_tx_nr_docto NOT LIKE '%EXTRA%'
        AND kp.sub_tipo IS NOT null
        {{ servicos() }}
        AND kp.kade_dh_ocorr
        BETWEEN TIMESTAMP '{{ start_parser.strftime("%Y-%m-%d 00:00:00.000") }}'
        AND TIMESTAMP '{{ end_parser.strftime("%Y-%m-%d 23:59:59.999") }}'
),

add_tipo_perdas AS (
    SELECT
        filial,
        kade_cd_produto AS cod_prod,
        DATE_TRUNC('month', kade_dh_ocorr) AS periodo,
        {%- for coluna, eventos in tipos %}
            SUM(kade_qt_mov) FILTER (
                WHERE sub_tipo {{ eventos }}
            ) AS qtd_{{ coluna }}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %},
        {%- for coluna, eventos in tipos %}
            SUM(kade_qt_mov * kade_vl_cmpcsicms) FILTER (
                WHERE sub_tipo {{ eventos }}
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
        'deposito' AS origem,
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
