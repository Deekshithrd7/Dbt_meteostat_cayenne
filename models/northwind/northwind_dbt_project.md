# Northwind dbt Project

This project is an analytics pipeline (ELT) built on top of the tables stored in a PostgreSQL database inside a dedicated northwind schema. The main goal was to take raw operational data such as orders, order_details, products and categories, and progressively clean and transform it using dbt into structured business-ready datasets.

---

## What problem this project solves

The data was stored in a PostgreSQL schema called northwind and consisted of multiple transactional tables including orders, order_details, products, and categories. While this structure is ideal for capturing operational data, it is not suitable for analysis or reporting without transformation.

The main problem this project solves is:

> How do we turn raw order-level data into clear business insights about sales performance, product categories, and revenue trends over time?

So instead of asking:
- “What happened in order 10248?”

We can answer:
- “Which product categories are performing best over time?”
- “How much revenue do we generate per month?”
- “Which products drive the most sales?”

This is the shift from operational data → analytical data.

---

## Models built in this project

The project is structured in three layers: staging, prep, and marts. Each layer has a clear purpose.

### Staging layer
This is the cleaning layer. The goal here is to take raw source tables and make them usable.

- `staging_orders`: cleans order-level data (dates, customer IDs, shipping details)
- `staging_order_details`: standardises product-level order line items
- `staging_products`: cleans product attributes like price, stock, category IDs
- `staging_categories`: provides clean category definitions

At this stage, the data is not transformed logically — just cleaned and standardized.

---

### Prep layer
This is where the real data modeling starts. Here we join multiple staging tables and create business-ready datasets.

- `prep_sales`: combines orders, order details, products, and categories
  - calculates revenue per line item
  - extracts time attributes (year, month)
  - creates a unified sales dataset at transaction level

This model is the foundation for all reporting logic later.

---

### Mart layer
This is the final business layer used for reporting and dashboards.

- `mart_sales_performance`: aggregates sales data by year, month, and product category
  - total revenue
  - total orders
  - average revenue per order

This is the layer that business users would actually interact with.

---

## Insights this mart provides to Northwind

The final mart makes it possible to answer real business questions like:

- Which product categories generate the highest revenue?
- How do sales trends change month by month?
- Are we seeing growth or decline in specific categories?
- What is the average order value per category over time?

Instead of working with raw transaction rows, we can see a clean, aggregated view of performance that is easy to interpret and ready to make insightful visuals.

---

## Biggest learning moment

The biggest learning from this project was understanding how dbt separates work into layers.

At first, I tried doing everything in one query, but splitting it into staging, prep, and marts made things much clearer and easier to manage.

Staging is for cleaning data, prep is for building logic and joins, and marts is for creating final business metrics.

I also learned that testing and documentation are not optional — they make the data more reliable and easier to trust.

Overall, the main shift was going from just writing SQL queries to actually designing a structured data model.
