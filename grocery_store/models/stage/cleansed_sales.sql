WITH raw_sales AS (
  SELECT * FROM {{ ref('src_sales') }}
)

SELECT     TRANSACTION_ID,
     TRANSACTIONAL_DATE,
     PRODUCT_ID,
     CUSTOMER_ID,
     PAYMENT,
     CREDIT_CARD,
     LOYALTY_CARD,
     COST,
     QUANTITY,
     PRICE,
     COST * QUANTITY AS TOTAL_COST,
     PRICE * QUANTITY AS TOTAL_PRICE,
     TOTAL_PRICE - TOTAL_COST AS TOTAL_PROFIT

FROM raw_sales