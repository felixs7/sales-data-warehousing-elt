WITH raw_sales as (
    SELECT * FROM {{ source ('grocery_store', 'sales') }}
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
     PRICE
FROM raw_sales


