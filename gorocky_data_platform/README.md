# Data Modelling using DBT

## Models

![image](https://github.com/user-attachments/assets/4c070875-2eed-4ee2-bd5d-9c7cad8cc89a)

The models are separated into 3 layers
- base layer. This is where we expose the data source in dbt project. The name of the model is `base_orders.sql`
- staging layer. This is where we clean the column and explicitly cast the appropriate data type on each column. The name of the model is `stg_orders.sql`
- mart layer. This is where we expose the end result of the cleaned model to end users. We used 3 mart models. We have 2 dimensional models `dim_customers.sql` and `dim_products.sql` which is extracted from the events table. We considered querying the latest info about the customer and the product. We have 1 fact table which is `mart_fct_order_transactions.sql`

## Test the models
- We have a Python script named `duckdb_test.py` that exports the mart model as a CSV. We use CSV as a data source on our dashboard

## Insights
- Fasteners has the lowest profit made (product sub-category) while Phones has the highest profit
- There are 793 unique customers on a given dataset
- The store wasn't really doing well due to high operational cost
- California has the highest made profit while Tennesee has the lowest made profit
- California also has the highest orders made while Florida has the lowest
- The peak profit made was recorded on September 23, 2014 and has a profit of 4.6K USD
- The range of date was recorded from Jan 3, 2014 upto Dec 31, 2014
- Most orders are shipped using Standard Class, while the least are in the same day mode
- Most orders are done from the East Region while the South has the lowest orders made

