{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        format='parquet',
        write_compression='zstd',
        table_type='iceberg',
        partitioned_by=['month(periodo)'],
        table_properties={"optimize_rewrite_delete_file_threshold": "2"},
        pre_hook=[
            "{% if is_incremental(name = this.identifier ) %} DELETE FROM {{ this }} WHERE periodo between TIMESTAMP '{{ var('start') }} 00:00:00.000' and TIMESTAMP '{{ var('end') }} 23:59:59.999' {% endif %}"
        ],
        post_hook=[
            "OPTIMIZE {{ this.render_pure() }} REWRITE DATA USING BIN_PACK",
            "VACUUM {{ this.render_pure() }}",
        ],
    )
}}

WITH final AS (
    SELECT * FROM {{ ref('stg_perdas_filial') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_perdas_deposito') }}
)

SELECT * FROM final
