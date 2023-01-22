 {{
    config(
        incremental=True,
        materialized='table',
        unique_key=["payment", "loyalty_card"],
        depends_on=['cleansed_sales']
    )
}}
 
 
 WITH cleansed_sales AS (
  SELECT *
  FROM
    {{ ref('cleansed_sales') }}
)

, distinct_combinations as (SELECT DISTINCT NVL(payment, 'cash') as payment, loyalty_card
FROM cleansed_sales
)

select {{ dbt_utils.surrogate_key(['payment', 'loyalty_card' ]) }} as payment_id, *
from distinct_combinations