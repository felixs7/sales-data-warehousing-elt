 {{
    config(
        incremental=True,
        materialized='table',
        unique_key=["transaction_id"],
        depends_on=['cleansed_sales, dim_payment', 'dim_product']
    )
}}
 
 
 WITH cleansed_sales AS (
  SELECT *
  FROM
    {{ ref('cleansed_sales') }}
),
p AS (
    select * from {{ ref('dim_payment') }}
),
dim_product as (
    select * from {{ ref('dim_product') }}
)


select transaction_id,
    TRANSACTIONAL_DATE,
    pro.product_key,
    CUSTOMER_ID,
    p.payment_id,
    credit_card,
    cost,
    quantity,
    price,
    total_cost,
    total_price,
    total_profit
from cleansed_sales cs
left join p ON NVL(cs.payment, 'cash') =  p.payment
            AND cs.loyalty_card = p.loyalty_card
left join dim_product pro ON cs.product_id =  pro.product_id