###  Mini Project: 
# 🧭 Northwind Sales Insights with dbt

Welcome to your first independent dbt project 🎉  
You’ve been learning how to clean, transform, and structure data using dbt — now it’s time to put that knowledge into practice!

----

## 📦 Business Scenario

You’ve joined **Northwind Trading**, a company that distributes food and beverage products worldwide.
They store their operational data in several raw tables in a warehouse (see the schema: northwind).

However, the analytics team has three big problems:

- The raw data is messy — column names and data types are inconsistent.

- Dashboards take too long to load because every analyst writes long SQL joins (too many manual joins).

- Everyone calculates “revenue” and “profit” differently!

Your task is to build a dbt project that cleans, enriches, and aggregates the data into clear, business-ready tables.


----
##  Project Goals
| Layer       | Purpose                                 | Example Model                |
| ----------- | --------------------------------------- | ---------------------------- |
| **Staging** | Clean and rename raw data               | `staging_orders.sql`         |
| **Prep**    | Add calculated fields, joins, and logic | `prep_sales.sql`             |
| **Marts**   | Aggregate for analysis and KPIs         | `mart_sales_performance.sql` |

You’ll also add tests and optional short documentation.

----
## Dataset Overview

All tables are already available in your database, however we will only use 4 tables as our data source:

1. orders
2. order_details
3. products
4. categories


---

## Setup your project 

Please use the same dbt project and git repo already setup for dbt_meteostat.

Presteps:
1. Add an additional subfolder `northwind` to the folder `models`.  
  **NOTE:** We will keep all models (staging, prep and mart in one folder.)
2. In the `dbt_project.yml` file add an instruction to materialize the models of the  "northwind" subfolderas as tables. Otherwise your results would be views. Pay attention to indentations!

    <details> <summary>Click to show a hint</summary>

    ```yml
    # Applies to all files under models/northwind/
    northwind:
      +materialized: table
    ```
    </details>
    <br>

3. Inside subfolder "northwind" add a new `staging_sources.yml` file where you define a new source and the inital tables. 
   
   <details> <summary>Click to show solution for staging_sources.yml</summary>

    ```yml
    version: 2
    sources:

    - name: northwind_data
      schema: northwind
      tables:
      - name: orders
      - name: order_details
      - name: categories
      - name: products
    ```

    </details>

----



## Step 1 — Staging Models
Goal: Clean and standardize raw data from the tables.

Create the following staging models in your models/northwind/ folder:

```yaml
staging_customers.sql
staging_orders.sql
staging_order_details.sql
staging_products.sql
```

Each should:

- Use `{{ source() }}` to pull data from the raw tables.

- Rename columns to snake_case.

- Cast dates and numbers to the correct types.

- Keep only relevant columns.

<details> <summary>Click to reveal hints</summary>

- Use Example : `{{ source('northwind', 'orders') }}` 

- Always alias columns in lowercase with underscores.

- Cast date fields using ::date.

- Only keep columns you actually need in later layers (e.g. order_id, order_date, customer_id).

- Make sure your dbt_project.yml has materialization set to table.

</details>


<details> <summary>Click to show solution staging_orders.sql</summary>

```sql
WITH source_data AS (
    SELECT *
    FROM {{ source('northwind', 'orders') }}
)
SELECT
    orderid AS order_id
    ,customerid AS customer_id
    ,employeeid AS employee_id
    ,orderdate::DATE AS order_date
    ,requireddate::DATE AS required_date
    ,shippeddate::DATE AS shipped_date
    ,shipvia AS ship_via
--	,freight
--	,shipname AS ship_name
--	,shipadress AS ship_address
    ,shipcity AS ship_city
--	,shipregion AS ship_region
--	,shippostalcode AS ship_postalcode
    ,shipcountry
FROM source_data
```

</details>


<details> <summary>Click to show solution staging_order_details.sql</summary>

```sql
WITH source_data AS (
    SELECT *
    FROM {{ source('northwind', 'order_details') }}
)
SELECT
    orderid AS order_id
    ,productid AS product_id
    ,unitprice::NUMERIC AS unit_price
    ,quantity::INT AS quantity
    ,discount::NUMERIC AS discount
FROM source_data
```
</details>

<details> <summary>Click to show solution staging_products.sql</summary>

```sql
WITH source_data AS (
    SELECT *
    FROM {{ source('northwind', 'products') }}
)
SELECT
    productid AS product_id
    ,productname product_name
    ,supplierid AS supplier_id
    ,categoryid AS category_id
--	,quantityperunit AS quantity_per_unit
    ,unitprice::NUMERIC AS unit_price
--	,unitsinstock::INT AS units_in_stock
--	,unitsonorder::INT AS units_on_order
--	,discontinued
FROM source_data
```

</details>

<details> <summary>Click to show solution staging_categories.sql</summary>

```sql
WITH source_data AS (
    SELECT *
    FROM {{ source('northwind', 'categories') }}
)
SELECT 
	categoryid AS category_id
	,categoryname AS category_name
--	,description
--	,picture
FROM source_data
```

</details>

----

## Step 2 — Prep Model
Goal: Join your clean staging tables and calculate key business metrics.

Create a new file:

- `models/prep/prep_sales.sql`


This model will:

- Join staging_orders, staging_order_details, and staging_products

- Calculate new columns:

  - revenue = unit_price * quantity * (1 - discount)

  - order_year, order_month

- (Optional) Add category_name by joining to staging_categories

This is where “business logic” starts.


<details> <summary>Click to reveal hints</summary>

- Use `{{ ref('staging_orders') }}` to reference your staging models.

- You can join on `order_id` and `product_id`.

- For order_year use:
  - `EXTRACT(year FROM order_date)` or
  - `DATE_PART('year', order_date)`

- Keep only relevant columns!

- This model should now start to look like a single “sales” dataset.

</details>


<details> <summary>Click to show sample solution prep_sales.sql</summary>

```sql
WITH orders AS (
    SELECT * FROM {{ ref('staging_orders') }}
),
order_details as (
    SELECT * FROM {{ ref('staging_order_details') }}
),
products as (
    SELECT * FROM {{ ref('staging_products') }}
),
categories as (
    SELECT * FROM {{ ref('staging_categories') }}
),
joined as (
    SELECT
        o.order_id,
        o.customer_id,
        p.product_name,
        c.category_name,
        od.unit_price,
        od.quantity,
        od.discount,
        (od.unit_price * od.quantity * (1 - od.discount)) AS revenue,
        EXTRACT(year FROM  o.order_date) AS order_year,
        EXTRACT(month FROM o.order_date) AS order_month
    FROM orders o
    JOIN order_details od USING (order_id)
    JOIN products p USING (product_id)
    LEFT JOIN categories c USING (category_id)
)
SELECT * FROM joined
```

</details>



---- 

## Step 3 — Mart Model
Goal: Create a summarized table for sales performance over time.

Create:

`models/marts/mart_sales_performance.sql`


This should:

Aggregate by order_year, order_month, and category_name

Show:

- total revenue

- total number of orders

- average revenue per order

#### This is the layer that a BI tool would use.


<details> <summary>Click to reveal hints</summary>

- query now from prep_sales model → use `{{ ref('prep_sales') }}`

- Use sum(), count(distinct order_id), avg()

- there is a shorter alternative to the `GROUP BY first_column, second_column, third_column` : instead you can use
    → `GROUP BY 1, 2, 3`

- You can order the output for readability

</details>


<details> <summary>Click to show solution mart_sales_performance.sql</summary>

```sql
WITH sales AS (
    SELECT * FROM {{ref('prep_sales')}}
)
SELECT
    order_year,
    order_month,
    category_name,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    AVG(revenue) AS avg_revenue_per_order
FROM sales
GROUP BY 1, 2, 3
ORDER BY category_name, order_year, order_month
```

</details>

----

## (OPTIONAL) Step 4 — Testing and Documentation
>**NOTE:** We haven't covered it. Hence here is an introduction video on tests and data validation:
>(for this exercise [**watch only until 3:26**](https://www.youtube.com/watch?v=L5X1NlUvfbc))
>
>Here we will focus on **basic validation** of our models. 

Goal: Make sure your final model is correct and documented.

Create a file:

`models/marts/schema.yml`


<details> <summary> Click to reveal hints</summary>

- Use YAML indentation carefully (2 spaces per level).

- Use tests: [not_null, unique] for important keys.

- You can describe each column for clarity.

</details>

#### Example Solution —  mart schema.yml
<details> <summary>Click to show solution schema.yml</summary>

``` yaml
version: 2

models:
  - name: mart_sales_performance
    description: "Monthly sales performance by category"
    columns:
      - name: order_year
        description: "Year when the order was placed"
        tests: [not_null]
      - name: order_month
        description: "Month when the order was placed"
        tests: [not_null]
      - name: category_name
        description: "Product category"
        tests: [not_null]
      - name: total_revenue
        description: "Sum of revenue for that period and category"
        tests: [not_null]
```
other test options:

```yaml
models:
    - ...
      ...
    columns:
        -   ...
            ...
            tests:
                - not_null
                - unique
                - [not_null, unique]
                - accepted_values:
                    values: ['pending', 'shipped', 'delivered', 'cancelled']
                - relationships:
                    to: ref('customers')
                    field: id
                - unique_combination_of_columns:
                    combination_of_columns: ['customer_id', 'status']
```
</details>

<br>

After creating tables in you DB with `dbt run`
You can run the validation tests with `dbt test`

<details> <summary>example result</summary>

![alt text](test_success.png)
</details>

<br>

More on tests and data validation:  
https://www.youtube.com/watch?v=L5X1NlUvfbc

----

## Step 5 — Reflection

Create a short README.md in your own project folder answering:

- What business problem does your dbt model solve?

- Which models did you build, and what does each do?

- What insights can your mart provide to Northwind?

- What was your biggest learning moment in this project?


