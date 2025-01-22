# Data Modelling using DBT

## Models

![image](https://github.com/user-attachments/assets/4c070875-2eed-4ee2-bd5d-9c7cad8cc89a)

The models are separated into 3 layers
- base layer. This is where we expose the data source in dbt project 
- staging layer. This is where we clean the column and explicitly cast the appropriate data type on each column
- mart layer. This is where we expose the end result of the cleaned model to end users

## Insights
- Fasteners has the lowest profit made (product sub-category) while Phones has the highest profit
- There are 793 unique customers on a given dataset
- The store wasn't really doing well due to high operational cost
- California has the highest made profit while Tennesee has the lowest made profit
- California also has the highest orders made while Florida has the lowest
- Most orders are shipped using Standard Class, while the least are in the same day mode
- Most orders are done from the East Region while the South has the lowest orders made

