WITH view_forn_pai AS (
    SELECT
        pai.data_hora_cadastro,
        filho.codigo_fornecedor,
        pai.codigo_fornecedor_principal
    FROM
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_grupo_fornecedores_aporte_cab') }}
            AS pai
    INNER JOIN
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_grupo_fornecedores_aporte_det') }}
            AS filho
        ON
            pai.id_grupo_fornecedores_aporte_cab
            = filho.id_grupo_fornecedores_aporte_cab
),

add_row_number AS (
    SELECT
        *,
        row_number()
            OVER (
                PARTITION BY codigo_fornecedor
                ORDER BY data_hora_cadastro DESC
            )
            AS id
    FROM view_forn_pai
),

drop_duplicates AS (
    SELECT
        codigo_fornecedor,
        codigo_fornecedor_principal
    FROM add_row_number
    WHERE id = 1
),

final AS (
    SELECT
        codigo_fornecedor AS cod_forn,
        codigo_fornecedor_principal AS cod_forn_pai,
        {{ strip_normalize("forn_nm_fantasia") }} AS forn_nm_pai,
        {{ cnpj_normalize("forn_tn_cnpj") }} AS cnpj_forn_pai
    FROM drop_duplicates AS pai
    INNER JOIN
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_fornecedor') }} AS forn
        ON pai.codigo_fornecedor_principal = forn.forn_cd_fornecedor
)

SELECT * FROM final
