
WITH src_products AS (
  SELECT
    product_id,
     product_name,
     category,
     subcategory
  FROM
    {{ ref('src_products') }}
)

SELECT product_id,
      TRIM(product_name) as cleansed_product_name,
      TRIM(SUBSTRING(product_name, CHARINDEX('(', product_name) +1 , CHARINDEX(')', product_name) - CHARINDEX('(', product_name) -1 )) AS Brandname,
      category,
      subcategory
FROM 
  src_products