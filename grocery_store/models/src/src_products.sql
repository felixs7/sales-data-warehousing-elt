WITH raw_products as (
    SELECT * FROM {{ source ('grocery_store', 'products') }}
)

SELECT     product_id,
     product_name,
     category,
     subcategory
FROM raw_products
