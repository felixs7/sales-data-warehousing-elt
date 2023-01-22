 {{
    config(
        incremental=True,
        materialized='table',
        unique_key=["product_id"],
        depends_on=['cleansed_product']
    )
}}

 WITH cp AS (
  SELECT *
  FROM
    {{ ref('cleansed_products') }}
)

select {{ dbt_utils.surrogate_key(['product_id' ]) }} as product_key, *
from cp