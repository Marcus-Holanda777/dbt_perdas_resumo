WITH view_forn_comercial AS (
    SELECT
        "codigo fornecedor principal deposito" AS cod_forn,
        "fornecedor comercial" AS forn_comercial
    FROM {{ source('planejamento_comercial', 'dim_produtos') }}
),

renamed AS (
    SELECT
        CAST(COALESCE(TRY_CAST(cod_forn AS DOUBLE), 0) AS INT) AS cod_forn,
        {{ strip_normalize("forn_comercial") }} AS forn_comercial
    FROM view_forn_comercial
),

filter_forn AS (
    SELECT * FROM renamed
    WHERE cod_forn > 0
),

add_duplicates AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cod_forn) AS id
    FROM filter_forn
),

final AS (
    SELECT
        cod_forn,
        forn_comercial
    FROM add_duplicates
    WHERE id = 1
)

SELECT * FROM final
