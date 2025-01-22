# Superstore Order Analysis

The process involved in the project is shown in the image below
![image](https://github.com/user-attachments/assets/6111c7ab-67f7-4ae0-bc54-beda27f34e16)
- We load the given CSV file and use dbt seed to load it on a dbt project
- We transform the CSV file into medallion layers (base, staging, and mart model)
- We test the transformed model using a Python script that connects to DuckDB
- We export the tables as CSV files and use those files as a data source on Looker Studio (the dashboard platform used)

The output of the assessment is a dashboard. 

It can be accessed on this dashboard link

[Dashboard](https://lookerstudio.google.com/reporting/87cc1617-b2be-4219-8094-064c82b2439c)

This is a screenshot of the dashboard for order analysis
![image](https://github.com/user-attachments/assets/0c4692dd-e841-4683-9320-e252b46eb3dd)
![image](https://github.com/user-attachments/assets/c001cb3b-e6bc-4e00-b857-87c2e00ae3c3)

The dashboard contains the following charts
- Daily trend of sales, profit and costs
- Overview of important metrics such as number of customers and total financial metrics considered
- Total Profit by Product Sub Category
- Total Number of Orders by State
- Sales VS Costs vs Profit by State
- Total Orders by Ship Mode and Region
- Details of the Order Transaction

This is the data lineage of the models involved in the dashboard
![image](https://github.com/user-attachments/assets/4c070875-2eed-4ee2-bd5d-9c7cad8cc89a)

So the philosophy of the data modelling is
- expose the data source in dbt project (base layer)
- clean the column and explicitly cast the appropriate data type on each column (staging layer)
- expose the end result of the cleaned model to end users (mart model)

The SQL files and Python scripts used in this project can be accessed in this link

[SQL and Python files](https://github.com/jddeguia/gorocky-store-analysis/tree/main/gorocky_data_platform)
