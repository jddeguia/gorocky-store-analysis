import duckdb

# Connect to the DuckDB database
conn = duckdb.connect(database='./dev.duckdb', read_only=False)

# Your query
query = "SELECT * FROM dim_customers LIMIT 10"

# Execute the query and save the result to a CSV file
conn.execute(query).df().to_csv('output.csv', index=False)

print("Data exported to output.csv")
