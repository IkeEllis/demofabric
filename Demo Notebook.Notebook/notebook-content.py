# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Import necessary libraries
from pyspark.sql import SparkSession

# Define connection parameters
jdbc_url = "jdbc:sqlserver://YaleDataSource"
connection_properties = {
    "user": "ike",
    "password": "getthatdata",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Example: read table 'your_table_name' - replace with your table
table_name = "your_table_name"

# Load data from SQL Server
df = spark.read.jdbc(
    url=jdbc_url,
    table=table_name,
    properties=connection_properties
)

# Show the data
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Save the Spark DataFrame 'df' to a new table named 'new_table'
df.write.format("delta").saveAsTable(f"new_table")

# If you want to overwrite the table if it already exists, use the following instead:
# df.write.mode("overwrite").format("delta").saveAsTable(f"new_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
