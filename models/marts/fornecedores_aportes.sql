{{
    config(
        materialized='table',
        format='parquet',
        write_compression='ZSTD'
    )
}}

WITH view_fornecedores AS (
    SELECT
        forn_cd_fornecedor AS cod_forn,
        {{ strip_normalize("forn_nm_fantasia") }} AS forn_nm,
        {{ cnpj_normalize("forn_tn_cnpj") }} AS cnpj_forn
    FROM {{ source('prevencao-perdas', 'cosmos_v14b_dbo_fornecedor') }}
),

join_pai AS (
    SELECT
        forn.*,
        coalesce(pai.cod_forn_pai, forn.cod_forn) AS cod_forn_pai,
        coalesce(pai.forn_nm_pai, forn.forn_nm) AS forn_nm_pai,
        coalesce(pai.cnpj_forn_pai, forn.cnpj_forn) AS cnpj_forn_pai
    FROM view_fornecedores AS forn
    LEFT JOIN
        {{ ref('stg_fornecedor_pai') }} AS pai
        ON forn.cod_forn = pai.cod_forn
),

join_comercial AS (
    SELECT
        pai.*,
        comercial.forn_comercial
    FROM join_pai AS pai
    LEFT JOIN
        {{ ref('stg_fornecedor_comercial') }} AS comercial
        ON pai.cod_forn = comercial.cod_forn
),

join_sap AS (
    SELECT
        comercial.*,
        sap.fornecedor_principal_sap
    FROM join_comercial AS comercial
    LEFT JOIN
        {{ ref('stg_fornecedor_sap') }} AS sap
        ON comercial.cod_forn = sap.forn_cd_fornecedor
),

final AS (
    SELECT
        cod_forn,
        forn_nm,
        cnpj_forn,
        cod_forn_pai,
        forn_nm_pai,
        cnpj_forn_pai,
        forn_comercial,
        fornecedor_principal_sap
    FROM join_sap
)

SELECT * FROM final
