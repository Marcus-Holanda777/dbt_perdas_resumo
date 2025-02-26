WITH view_forn_sap AS (
    SELECT
        forn_cd_fornecedor,
        fsma_codigo_sap_master,
        xxxx_dh_cad
    FROM {{ source('prevencao-perdas', 'cosmos_v14b_dbo_fornecedor') }}
    WHERE fsma_codigo_sap_master IS NOT null
),

add_duplicate AS (
    SELECT
        *,
        row_number()
            OVER (
                PARTITION BY fsma_codigo_sap_master
                ORDER BY xxxx_dh_cad DESC
            )
            AS id
    FROM view_forn_sap
),

drop_duplicate AS (
    SELECT *
    FROM add_duplicate
    WHERE id = 1
),

final AS (
    SELECT
        fsma_codigo_sap_master AS fornecedor_principal_sap,
        forn_cd_fornecedor
    FROM drop_duplicate
)

SELECT * FROM final
