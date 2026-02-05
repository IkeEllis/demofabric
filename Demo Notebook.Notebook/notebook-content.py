# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cc9c8a10-7f1d-4d7b-9ce1-ea7923bda6d6",
# META       "default_lakehouse_name": "demolakehouse",
# META       "default_lakehouse_workspace_id": "85cf6401-8eeb-419d-9a64-acfe3164d82d",
# META       "known_lakehouses": [
# META         {
# META           "id": "cc9c8a10-7f1d-4d7b-9ce1-ea7923bda6d6"
# META         }
# META       ]
# META     }
# META   }
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

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Delete (drop) the table named 'new_table' if it exists
spark.sql("DROP TABLE IF EXISTS new_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Create customers table
# MAGIC CREATE OR REPLACE TABLE testschema.customers (
# MAGIC     customer_id BIGINT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     date_of_birth DATE,
# MAGIC     gender STRING,
# MAGIC     address_line1 STRING,
# MAGIC     address_line2 STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     zip_code STRING,
# MAGIC     country STRING,
# MAGIC     registration_date TIMESTAMP,
# MAGIC     last_login_date TIMESTAMP,
# MAGIC     account_status STRING,
# MAGIC     customer_segment STRING,
# MAGIC     lifetime_value DECIMAL(10,2),
# MAGIC     total_orders INT,
# MAGIC     preferred_contact_method STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC -- Insert 200 dummy records
# MAGIC INSERT INTO testschema.customers  VALUES
# MAGIC -- Records 1-50
# MAGIC (1, 'John', 'Smith', 'john.smith@email.com', '555-0101', '1985-03-15', 'M', '123 Main St', 'Apt 4B', 'New York', 'NY', '10001', 'USA', '2020-01-15 10:30:00', '2024-01-20 14:22:00', 'Active', 'Premium', 15420.50, 45, 'Email'),
# MAGIC (2, 'Sarah', 'Johnson', 'sarah.j@email.com', '555-0102', '1990-07-22', 'F', '456 Oak Ave', NULL, 'Los Angeles', 'CA', '90001', 'USA', '2020-02-20 09:15:00', '2024-01-19 11:45:00', 'Active', 'Standard', 8750.25, 28, 'SMS'),
# MAGIC (3, 'Michael', 'Williams', 'mwilliams@email.com', '555-0103', '1978-11-30', 'M', '789 Pine Rd', 'Suite 200', 'Chicago', 'IL', '60601', 'USA', '2019-05-10 13:45:00', '2024-01-18 16:30:00', 'Active', 'Premium', 22100.75, 67, 'Phone'),
# MAGIC (4, 'Emily', 'Brown', 'ebrown@email.com', '555-0104', '1995-02-14', 'F', '321 Elm St', NULL, 'Houston', 'TX', '77001', 'USA', '2021-03-25 11:20:00', '2024-01-21 09:15:00', 'Active', 'Standard', 4320.00, 15, 'Email'),
# MAGIC (5, 'David', 'Jones', 'djones@email.com', '555-0105', '1982-09-08', 'M', '654 Maple Dr', 'Unit 3', 'Phoenix', 'AZ', '85001', 'USA', '2019-11-30 14:50:00', '2024-01-17 13:20:00', 'Inactive', 'Basic', 1250.50, 8, 'Email'),
# MAGIC (6, 'Jessica', 'Garcia', 'jgarcia@email.com', '555-0106', '1988-06-25', 'F', '987 Cedar Ln', NULL, 'Philadelphia', 'PA', '19101', 'USA', '2020-07-12 10:05:00', '2024-01-22 10:40:00', 'Active', 'Premium', 18900.00, 52, 'SMS'),
# MAGIC (7, 'James', 'Martinez', 'jmartinez@email.com', '555-0107', '1992-12-03', 'M', '147 Birch Way', 'Apt 12', 'San Antonio', 'TX', '78201', 'USA', '2021-01-08 15:30:00', '2024-01-16 12:55:00', 'Active', 'Standard', 6780.25, 22, 'Email'),
# MAGIC (8, 'Jennifer', 'Rodriguez', 'jrodriguez@email.com', '555-0108', '1987-04-17', 'F', '258 Willow Ct', NULL, 'San Diego', 'CA', '92101', 'USA', '2020-09-22 09:40:00', '2024-01-20 15:10:00', 'Active', 'Premium', 21450.50, 61, 'Phone'),
# MAGIC (9, 'Robert', 'Hernandez', 'rhernandez@email.com', '555-0109', '1980-08-11', 'M', '369 Spruce Blvd', 'Suite 5', 'Dallas', 'TX', '75201', 'USA', '2019-03-15 12:25:00', '2023-12-28 14:30:00', 'Active', 'Standard', 9340.75, 31, 'Email'),
# MAGIC (10, 'Mary', 'Lopez', 'mlopez@email.com', '555-0110', '1993-01-29', 'F', '741 Ash Ave', NULL, 'San Jose', 'CA', '95101', 'USA', '2021-06-14 11:55:00', '2024-01-19 09:25:00', 'Active', 'Basic', 3210.00, 12, 'SMS'),
# MAGIC (11, 'Christopher', 'Gonzalez', 'cgonzalez@email.com', '555-0111', '1986-05-20', 'M', '852 Palm St', 'Apt 7A', 'Austin', 'TX', '78701', 'USA', '2020-04-18 10:15:00', '2024-01-21 11:40:00', 'Active', 'Premium', 17650.25, 48, 'Email'),
# MAGIC (12, 'Linda', 'Wilson', 'lwilson@email.com', '555-0112', '1991-10-07', 'F', '963 Fir Rd', NULL, 'Jacksonville', 'FL', '32099', 'USA', '2021-08-30 14:20:00', '2024-01-18 16:15:00', 'Active', 'Standard', 7890.50, 26, 'Phone'),
# MAGIC (13, 'Daniel', 'Anderson', 'danderson@email.com', '555-0113', '1984-03-12', 'M', '159 Poplar Dr', 'Unit 9', 'Columbus', 'OH', '43085', 'USA', '2019-12-05 13:10:00', '2024-01-17 10:30:00', 'Suspended', 'Basic', 890.00, 5, 'Email'),
# MAGIC (14, 'Barbara', 'Thomas', 'bthomas@email.com', '555-0114', '1989-07-28', 'F', '357 Hickory Ln', NULL, 'Fort Worth', 'TX', '76101', 'USA', '2020-10-16 09:50:00', '2024-01-22 13:05:00', 'Active', 'Premium', 19320.75, 55, 'SMS'),
# MAGIC (15, 'Matthew', 'Taylor', 'mtaylor@email.com', '555-0115', '1994-11-19', 'M', '486 Magnolia Ct', 'Apt 2C', 'Charlotte', 'NC', '28201', 'USA', '2021-02-28 11:35:00', '2024-01-20 14:50:00', 'Active', 'Standard', 5670.25, 19, 'Email'),
# MAGIC (16, 'Susan', 'Moore', 'smoore@email.com', '555-0116', '1981-09-05', 'F', '597 Sycamore Blvd', NULL, 'San Francisco', 'CA', '94101', 'USA', '2019-07-22 15:45:00', '2024-01-19 12:20:00', 'Active', 'Premium', 24150.00, 71, 'Phone'),
# MAGIC (17, 'Joseph', 'Jackson', 'jjackson@email.com', '555-0117', '1996-02-16', 'M', '618 Redwood Ave', 'Suite 12', 'Indianapolis', 'IN', '46201', 'USA', '2021-09-10 10:25:00', '2024-01-16 15:35:00', 'Active', 'Basic', 2780.50, 10, 'Email'),
# MAGIC (18, 'Margaret', 'White', 'mwhite@email.com', '555-0118', '1983-06-23', 'F', '729 Cypress St', NULL, 'Seattle', 'WA', '98101', 'USA', '2020-01-30 12:40:00', '2024-01-21 10:15:00', 'Active', 'Standard', 11230.75, 38, 'SMS'),
# MAGIC (19, 'Thomas', 'Harris', 'tharris@email.com', '555-0119', '1990-12-09', 'M', '840 Dogwood Rd', 'Apt 5B', 'Denver', 'CO', '80201', 'USA', '2021-04-07 14:05:00', '2024-01-18 09:45:00', 'Active', 'Premium', 16540.25, 46, 'Email'),
# MAGIC (20, 'Dorothy', 'Martin', 'dmartin@email.com', '555-0120', '1987-08-31', 'F', '951 Beech Dr', NULL, 'Washington', 'DC', '20001', 'USA', '2020-06-19 11:15:00', '2024-01-22 14:25:00', 'Active', 'Standard', 8920.00, 29, 'Phone'),
# MAGIC (21, 'Charles', 'Thompson', 'cthompson@email.com', '555-0121', '1985-01-14', 'M', '162 Walnut Way', 'Unit 6', 'Boston', 'MA', '02101', 'USA', '2019-08-28 13:50:00', '2024-01-17 11:30:00', 'Inactive', 'Basic', 1540.75, 7, 'Email'),
# MAGIC (22, 'Betty', 'Garcia', 'bgarcia@email.com', '555-0122', '1992-05-26', 'F', '273 Chestnut Ct', NULL, 'El Paso', 'TX', '79901', 'USA', '2021-07-15 10:30:00', '2024-01-20 16:10:00', 'Active', 'Premium', 20100.50, 58, 'SMS'),
# MAGIC (23, 'Steven', 'Martinez', 'smartinez@email.com', '555-0123', '1979-10-18', 'M', '384 Pecan Ln', 'Apt 8D', 'Nashville', 'TN', '37201', 'USA', '2019-11-12 15:20:00', '2024-01-19 13:45:00', 'Active', 'Standard', 10450.25, 35, 'Email'),
# MAGIC (24, 'Helen', 'Robinson', 'hrobinson@email.com', '555-0124', '1988-03-04', 'F', '495 Alder Blvd', NULL, 'Detroit', 'MI', '48201', 'USA', '2020-08-05 09:35:00', '2024-01-21 15:20:00', 'Active', 'Premium', 18230.00, 51, 'Phone'),
# MAGIC (25, 'Paul', 'Clark', 'pclark@email.com', '555-0125', '1995-07-21', 'M', '516 Juniper Ave', 'Suite 3', 'Memphis', 'TN', '37501', 'USA', '2021-10-22 12:10:00', '2024-01-18 10:55:00', 'Active', 'Basic', 3890.75, 13, 'Email'),
# MAGIC (26, 'Sandra', 'Rodriguez', 'srodriguez@email.com', '555-0126', '1984-11-13', 'F', '627 Laurel St', NULL, 'Portland', 'OR', '97201', 'USA', '2020-02-14 14:45:00', '2024-01-22 12:30:00', 'Active', 'Standard', 9560.50, 32, 'SMS'),
# MAGIC (27, 'Mark', 'Lewis', 'mlewis@email.com', '555-0127', '1991-04-08', 'M', '738 Aspen Rd', 'Apt 1A', 'Oklahoma City', 'OK', '73101', 'USA', '2021-05-18 11:25:00', '2024-01-17 14:40:00', 'Active', 'Premium', 17890.25, 49, 'Email'),
# MAGIC (28, 'Donna', 'Lee', 'dlee@email.com', '555-0128', '1986-09-27', 'F', '849 Cottonwood Dr', NULL, 'Las Vegas', 'NV', '89101', 'USA', '2020-03-09 10:55:00', '2024-01-20 09:20:00', 'Active', 'Standard', 7340.00, 24, 'Phone'),
# MAGIC (29, 'Donald', 'Walker', 'dwalker@email.com', '555-0129', '1993-02-12', 'M', '950 Mesquite Ln', 'Unit 4', 'Louisville', 'KY', '40201', 'USA', '2021-11-30 13:40:00', '2024-01-19 16:25:00', 'Active', 'Basic', 4120.75, 14, 'Email'),
# MAGIC (30, 'Carol', 'Hall', 'chall@email.com', '555-0130', '1982-06-19', 'F', '161 Sequoia Ct', NULL, 'Baltimore', 'MD', '21201', 'USA', '2019-09-17 12:05:00', '2024-01-21 11:15:00', 'Active', 'Premium', 21670.50, 63, 'SMS'),
# MAGIC (31, 'Kenneth', 'Allen', 'kallen@email.com', '555-0131', '1989-10-06', 'M', '272 Buckeye Blvd', 'Apt 9C', 'Milwaukee', 'WI', '53201', 'USA', '2020-12-23 14:30:00', '2024-01-18 13:50:00', 'Active', 'Standard', 8120.25, 27, 'Email'),
# MAGIC (32, 'Michelle', 'Young', 'myoung@email.com', '555-0132', '1994-12-24', 'F', '383 Ironwood Ave', NULL, 'Albuquerque', 'NM', '87101', 'USA', '2021-03-12 09:20:00', '2024-01-22 10:35:00', 'Active', 'Premium', 19780.00, 56, 'Phone'),
# MAGIC (33, 'Kevin', 'Hernandez', 'khernandez@email.com', '555-0133', '1980-05-11', 'M', '494 Maplewood St', 'Suite 7', 'Tucson', 'AZ', '85701', 'USA', '2019-06-25 15:15:00', '2024-01-17 12:10:00', 'Suspended', 'Basic', 760.50, 4, 'Email'),
# MAGIC (34, 'Lisa', 'King', 'lking@email.com', '555-0134', '1987-08-29', 'F', '515 Pinewood Rd', NULL, 'Fresno', 'CA', '93701', 'USA', '2020-05-07 11:40:00', '2024-01-20 15:45:00', 'Active', 'Standard', 10890.75, 36, 'SMS'),
# MAGIC (35, 'Brian', 'Wright', 'bwright@email.com', '555-0135', '1992-01-16', 'M', '626 Oakmont Dr', 'Apt 6B', 'Sacramento', 'CA', '94201', 'USA', '2021-08-19 10:50:00', '2024-01-19 09:30:00', 'Active', 'Premium', 16210.25, 44, 'Email'),
# MAGIC (36, 'Nancy', 'Lopez', 'nlopez@email.com', '555-0136', '1983-04-03', 'F', '737 Cedarbrook Ln', NULL, 'Mesa', 'AZ', '85201', 'USA', '2020-01-21 13:25:00', '2024-01-21 14:05:00', 'Active', 'Standard', 9780.50, 33, 'Phone'),
# MAGIC (37, 'George', 'Hill', 'ghill@email.com', '555-0137', '1990-07-18', 'M', '848 Elmhurst Ct', 'Unit 2', 'Kansas City', 'MO', '64101', 'USA', '2021-04-26 12:35:00', '2024-01-18 11:20:00', 'Active', 'Basic', 3560.00, 11, 'Email'),
# MAGIC (38, 'Karen', 'Scott', 'kscott@email.com', '555-0138', '1986-11-25', 'F', '959 Brookside Blvd', NULL, 'Atlanta', 'GA', '30301', 'USA', '2020-07-29 09:45:00', '2024-01-22 16:30:00', 'Active', 'Premium', 22450.75, 68, 'SMS'),
# MAGIC (39, 'Jason', 'Green', 'jgreen@email.com', '555-0139', '1995-03-09', 'M', '160 Riverside Ave', 'Apt 3D', 'Long Beach', 'CA', '90801', 'USA', '2021-12-08 14:15:00', '2024-01-17 10:40:00', 'Active', 'Standard', 5230.25, 17, 'Email'),
# MAGIC (40, 'Betty', 'Adams', 'badams@email.com', '555-0140', '1981-09-22', 'F', '271 Lakeview St', NULL, 'Colorado Springs', 'CO', '80901', 'USA', '2019-10-14 11:05:00', '2024-01-20 13:25:00', 'Active', 'Premium', 20890.50, 60, 'Phone'),
# MAGIC (41, 'Jeff', 'Baker', 'jbaker@email.com', '555-0141', '1988-02-07', 'M', '382 Hillside Rd', 'Suite 5', 'Raleigh', 'NC', '27601', 'USA', '2020-09-11 12:50:00', '2024-01-19 15:10:00', 'Active', 'Standard', 8540.00, 28, 'Email'),
# MAGIC (42, 'Angela', 'Gonzalez', 'agonzalez@email.com', '555-0142', '1993-06-14', 'F', '493 Sunset Dr', NULL, 'Omaha', 'NE', '68101', 'USA', '2021-01-27 10:20:00', '2024-01-21 12:45:00', 'Active', 'Premium', 18560.75, 53, 'SMS'),
# MAGIC (43, 'Timothy', 'Nelson', 'tnelson@email.com', '555-0143', '1979-12-01', 'M', '514 Mountain Ln', 'Apt 7C', 'Miami', 'FL', '33101', 'USA', '2019-05-19 15:30:00', '2024-01-18 09:55:00', 'Inactive', 'Basic', 1670.25, 9, 'Email'),
# MAGIC (44, 'Deborah', 'Carter', 'dcarter@email.com', '555-0144', '1985-08-16', 'F', '625 Forest Ct', NULL, 'Oakland', 'CA', '94601', 'USA', '2020-11-04 09:10:00', '2024-01-22 11:30:00', 'Active', 'Standard', 11450.50, 39, 'Phone'),
# MAGIC (45, 'Frank', 'Mitchell', 'fmitchell@email.com', '555-0145', '1991-11-28', 'M', '736 Valley Blvd', 'Unit 8', 'Minneapolis', 'MN', '55401', 'USA', '2021-06-03 13:45:00', '2024-01-17 14:20:00', 'Active', 'Premium', 17120.00, 47, 'Email'),
# MAGIC (46, 'Ruth', 'Perez', 'rperez@email.com', '555-0146', '1984-03-23', 'F', '847 Garden Ave', NULL, 'Tulsa', 'OK', '74101', 'USA', '2020-04-15 11:30:00', '2024-01-20 16:40:00', 'Active', 'Standard', 7650.75, 25, 'SMS'),
# MAGIC (47, 'Raymond', 'Roberts', 'rroberts@email.com', '555-0147', '1992-09-10', 'M', '958 Spring St', 'Apt 4A', 'Cleveland', 'OH', '44101', 'USA', '2021-09-28 10:40:00', '2024-01-19 13:15:00', 'Active', 'Basic', 4340.25, 15, 'Email'),
# MAGIC (48, 'Sharon', 'Turner', 'sturner@email.com', '555-0148', '1987-01-27', 'F', '169 Harbor Rd', NULL, 'Wichita', 'KS', '67201', 'USA', '2020-08-22 14:55:00', '2024-01-21 10:50:00', 'Active', 'Premium', 19450.50, 55, 'Phone'),
# MAGIC (49, 'Gregory', 'Phillips', 'gphillips@email.com', '555-0149', '1994-05-04', 'M', '270 Bay Dr', 'Suite 1', 'New Orleans', 'LA', '70112', 'USA', '2021-02-16 12:15:00', '2024-01-18 15:35:00', 'Active', 'Standard', 6890.00, 23, 'Email'),
# MAGIC (50, 'Cynthia', 'Campbell', 'ccampbell@email.com', '555-0150', '1980-10-21', 'F', '381 Beach Ln', NULL, 'Bakersfield', 'CA', '93301', 'USA', '2019-12-30 09:25:00', '2024-01-22 12:05:00', 'Active', 'Premium', 23340.75, 70, 'SMS'),
# MAGIC -- Records 51-100
# MAGIC (51, 'Eric', 'Parker', 'eparker@email.com', '555-0151', '1989-04-12', 'M', '492 Ocean Ct', 'Apt 2B', 'Tampa', 'FL', '33601', 'USA', '2020-10-08 11:45:00', '2024-01-17 09:40:00', 'Active', 'Standard', 9120.25, 30, 'Email'),
# MAGIC (52, 'Shirley', 'Evans', 'sevans@email.com', '555-0152', '1996-08-29', 'F', '513 River Blvd', NULL, 'Anaheim', 'CA', '92801', 'USA', '2022-01-14 10:30:00', '2024-01-20 14:55:00', 'Active', 'Basic', 2450.50, 8, 'Phone'),
# MAGIC (53, 'Adam', 'Edwards', 'aedwards@email.com', '555-0153', '1982-12-15', 'M', '624 Park Ave', 'Unit 5', 'Aurora', 'CO', '80010', 'USA', '2019-07-08 15:50:00', '2024-01-19 11:25:00', 'Active', 'Premium', 21230.00, 62, 'Email'),
# MAGIC (54, 'Virginia', 'Collins', 'vcollins@email.com', '555-0154', '1990-02-28', 'F', '735 Lake St', NULL, 'Santa Ana', 'CA', '92701', 'USA', '2021-03-21 12:20:00', '2024-01-21 16:10:00', 'Active', 'Standard', 8340.75, 27, 'SMS'),
# MAGIC (55, 'Jeremy', 'Stewart', 'jstewart@email.com', '555-0155', '1985-07-05', 'M', '846 Hill Rd', 'Apt 9A', 'St. Louis', 'MO', '63101', 'USA', '2020-05-26 09:35:00', '2024-01-18 13:40:00', 'Suspended', 'Basic', 920.25, 5, 'Email'),
# MAGIC (56, 'Brenda', 'Sanchez', 'bsanchez@email.com', '555-0156', '1993-11-18', 'F', '957 Grove Dr', NULL, 'Riverside', 'CA', '92501', 'USA', '2021-08-07 14:10:00', '2024-01-22 10:20:00', 'Active', 'Premium', 18890.50, 54, 'Phone'),
# MAGIC (57, 'Gary', 'Morris', 'gmorris@email.com', '555-0157', '1978-03-31', 'M', '168 Creek Ln', 'Suite 3', 'Corpus Christi', 'TX', '78401', 'USA', '2019-04-12 13:00:00', '2024-01-17 15:25:00', 'Active', 'Standard', 12340.00, 41, 'Email'),
# MAGIC (58, 'Pamela', 'Rogers', 'progers@email.com', '555-0158', '1988-09-07', 'F', '279 Stone Ct', NULL, 'Lexington', 'KY', '40502', 'USA', '2020-12-19 11:15:00', '2024-01-20 12:50:00', 'Active', 'Premium', 20560.75, 59, 'SMS'),
# MAGIC (59, 'Nicholas', 'Reed', 'nreed@email.com', '555-0159', '1995-01-24', 'M', '380 Ridge Blvd', 'Apt 6C', 'Pittsburgh', 'PA', '15201', 'USA', '2021-11-11 10:05:00', '2024-01-19 09:35:00', 'Active', 'Basic', 3780.25, 12, 'Email'),
# MAGIC (60, 'Catherine', 'Cook', 'ccook@email.com', '555-0160', '1981-06-11', 'F', '491 Bridge Ave', NULL, 'Stockton', 'CA', '95201', 'USA', '2019-09-28 12:40:00', '2024-01-21 14:15:00', 'Active', 'Standard', 10230.50, 34, 'Phone'),
# MAGIC (61, 'Aaron', 'Morgan', 'amorgan@email.com', '555-0161', '1987-10-19', 'M', '512 Trail St', 'Unit 1', 'Cincinnati', 'OH', '45201', 'USA', '2020-06-14 14:25:00', '2024-01-18 11:40:00', 'Active', 'Premium', 19120.00, 55, 'Email'),
# MAGIC (62, 'Christine', 'Bell', 'cbell@email.com', '555-0162', '1992-04-06', 'F', '623 Meadow Rd', NULL, 'Anchorage', 'AK', '99501', 'USA', '2021-01-19 09:50:00', '2024-01-22 16:05:00', 'Active', 'Standard', 7230.75, 24, 'SMS'),
# MAGIC (63, 'Henry', 'Murphy', 'hmurphy@email.com', '555-0163', '1983-08-23', 'M', '734 Field Dr', 'Apt 3C', 'Plano', 'TX', '75023', 'USA', '2020-03-05 11:20:00', '2024-01-17 13:30:00', 'Active', 'Basic', 4560.25, 16, 'Email'),
# MAGIC (64, 'Janet', 'Bailey', 'jbailey@email.com', '555-0164', '1990-12-10', 'F', '845 Pond Ln', NULL, 'Henderson', 'NV', '89002', 'USA', '2021-07-23 13:35:00', '2024-01-20 10:45:00', 'Active', 'Premium', 17780.50, 50, 'Phone'),
# MAGIC (65, 'Douglas', 'Rivera', 'drivera@email.com', '555-0165', '1986-02-17', 'M', '956 Dale Ct', 'Suite 8', 'Lincoln', 'NE', '68501', 'USA', '2020-09-01 10:10:00', '2024-01-19 15:20:00', 'Active', 'Standard', 8890.00, 29, 'Email'),
# MAGIC (66, 'Ann', 'Cooper', 'acooper@email.com', '555-0166', '1994-06-04', 'F', '167 Mill Blvd', NULL, 'Greensboro', 'NC', '27401', 'USA', '2021-10-30 12:55:00', '2024-01-21 12:15:00', 'Active', 'Premium', 16670.75, 45, 'SMS'),
# MAGIC (67, 'Peter', 'Richardson', 'prichardson@email.com', '555-0167', '1979-11-21', 'M', '278 Farm Ave', 'Apt 5D', 'Chandler', 'AZ', '85224', 'USA', '2019-08-16 15:40:00', '2024-01-18 09:50:00', 'Inactive', 'Basic', 1340.25, 7, 'Email'),
# MAGIC (68, 'Joyce', 'Cox', 'jcox@email.com', '555-0168', '1985-03-08', 'F', '389 Glen St', NULL, 'Gilbert', 'AZ', '85233', 'USA', '2020-11-27 09:30:00', '2024-01-22 14:35:00', 'Active', 'Standard', 11670.50, 40, 'Phone'),
# MAGIC (69, 'Walter', 'Howard', 'whoward@email.com', '555-0169', '1991-07-25', 'M', '490 Court Rd', 'Unit 2', 'Norfolk', 'VA', '23501', 'USA', '2021-04-15 11:45:00', '2024-01-17 16:25:00', 'Active', 'Premium', 18230.00, 52, 'Email'),
# MAGIC (70, 'Gloria', 'Ward', 'gward@email.com', '555-0170', '1984-11-02', 'F', '511 Plaza Dr', NULL, 'Reno', 'NV', '89501', 'USA', '2020-02-08 13:15:00', '2024-01-20 11:10:00', 'Active', 'Standard', 9450.75, 31, 'SMS'),
# MAGIC (71, 'Harold', 'Torres', 'htorres@email.com', '555-0171', '1992-09-19', 'M', '622 Square Ln', 'Apt 8B', 'Hialeah', 'FL', '33010', 'USA', '2021-06-22 10:25:00', '2024-01-19 13:55:00', 'Active', 'Basic', 3210.25, 11, 'Email'),
# MAGIC (72, 'Teresa', 'Peterson', 'tpeterson@email.com', '555-0172', '1988-01-06', 'F', '733 Market Ct', NULL, 'Garland', 'TX', '75040', 'USA', '2020-08-12 14:50:00', '2024-01-21 15:40:00', 'Active', 'Premium', 22120.50, 67, 'Phone'),
# MAGIC (73, 'Carl', 'Gray', 'cgray@email.com', '555-0173', '1995-05-13', 'M', '844 Center Blvd', 'Suite 4', 'Irving', 'TX', '75060', 'USA', '2021-12-16 12:05:00', '2024-01-18 10:30:00', 'Active', 'Standard', 5450.00, 18, 'Email'),
# MAGIC (74, 'Evelyn', 'Ramirez', 'eramirez@email.com', '555-0174', '1980-10-30', 'F', '955 Main Ave', NULL, 'Scottsdale', 'AZ', '85250', 'USA', '2019-05-23 09:20:00', '2024-01-22 12:45:00', 'Active', 'Premium', 24560.75, 72, 'SMS'),
# MAGIC (75, 'Arthur', 'James', 'ajames@email.com', '555-0175', '1987-02-16', 'M', '166 State St', 'Apt 1C', 'Chesapeake', 'VA', '23320', 'USA', '2020-07-19 11:35:00', '2024-01-17 14:05:00', 'Active', 'Standard', 8670.25, 28, 'Email'),
# MAGIC (76, 'Katherine', 'Watson', 'kwatson@email.com', '555-0176', '1993-08-03', 'F', '277 North Rd', NULL, 'North Las Vegas', 'NV', '89030', 'USA', '2021-03-29 13:50:00', '2024-01-20 16:20:00', 'Active', 'Premium', 17340.50, 48, 'Phone'),
# MAGIC (77, 'Wayne', 'Brooks', 'wbrooks@email.com', '555-0177', '1982-12-20', 'M', '388 South Dr', 'Unit 7', 'Boise', 'ID', '83702', 'USA', '2020-01-11 10:40:00', '2024-01-19 09:15:00', 'Suspended', 'Basic', 1120.00, 6, 'Email'),
# MAGIC (78, 'Diane', 'Kelly', 'dkelly@email.com', '555-0178', '1989-04-27', 'F', '499 East Ln', NULL, 'Laredo', 'TX', '78040', 'USA', '2020-10-24 12:25:00', '2024-01-21 11:50:00', 'Active', 'Standard', 10560.75, 37, 'SMS'),
# MAGIC (79, 'Willie', 'Sanders', 'wsanders@email.com', '555-0179', '1996-06-14', 'M', '510 West Ct', 'Apt 4B', 'Madison', 'WI', '53701', 'USA', '2022-02-05 09:15:00', '2024-01-18 15:35:00', 'Active', 'Basic', 2890.25, 9, 'Email'),
# MAGIC (80, 'Alice', 'Price', 'aprice@email.com', '555-0180', '1981-09-01', 'F', '621 Old Blvd', NULL, 'Lubbock', 'TX', '79401', 'USA', '2019-11-19 14:30:00', '2024-01-22 13:20:00', 'Active', 'Premium', 20340.50, 58, 'Phone'),
# MAGIC (81, 'Ralph', 'Bennett', 'rbennett@email.com', '555-0181', '1988-12-18', 'M', '732 New Ave', 'Suite 2', 'Chandler', 'AZ', '85225', 'USA', '2020-04-07 11:10:00', '2024-01-17 10:05:00', 'Active', 'Standard', 9780.00, 32, 'Email'),
# MAGIC (82, 'Judith', 'Wood', 'jwood@email.com', '555-0182', '1994-10-05', 'F', '843 First St', NULL, 'Durham', 'NC', '27701', 'USA', '2021-09-18 13:20:00', '2024-01-20 14:40:00', 'Active', 'Premium', 16890.75, 46, 'SMS'),
# MAGIC (83, 'Lawrence', 'Barnes', 'lbarnes@email.com', '555-0183', '1983-03-22', 'M', '954 Second Rd', 'Apt 6A', 'Winston-Salem', 'NC', '27101', 'USA', '2020-08-29 10:45:00', '2024-01-19 12:25:00', 'Active', 'Standard', 7890.25, 26, 'Email'),
# MAGIC (84, 'Jean', 'Ross', 'jross@email.com', '555-0184', '1990-07-09', 'F', '165 Third Dr', NULL, 'Laredo', 'TX', '78041', 'USA', '2021-05-07 12:30:00', '2024-01-21 16:50:00', 'Active', 'Premium', 19670.50, 57, 'Phone'),
# MAGIC (85, 'Eugene', 'Henderson', 'ehenderson@email.com', '555-0185', '1986-11-16', 'M', '276 Fourth Ln', 'Unit 3', 'Chula Vista', 'CA', '91910', 'USA', '2020-12-05 09:55:00', '2024-01-18 11:15:00', 'Active', 'Basic', 4670.00, 15, 'Email'),
# MAGIC (86, 'Carolyn', 'Coleman', 'ccoleman@email.com', '555-0186', '1992-01-03', 'F', '387 Fifth Ct', NULL, 'Buffalo', 'NY', '14201', 'USA', '2021-02-14 14:40:00', '2024-01-22 09:45:00', 'Active', 'Standard', 11230.75, 38, 'SMS'),
# MAGIC (87, 'Russell', 'Jenkins', 'rjenkins@email.com', '555-0187', '1979-05-20', 'M', '498 Sixth Blvd', 'Apt 7D', 'Orlando', 'FL', '32801', 'USA', '2019-10-06 15:25:00', '2024-01-17 13:05:00', 'Inactive', 'Basic', 1890.25, 8, 'Email'),
# MAGIC (88, 'Frances', 'Perry', 'fperry@email.com', '555-0188', '1985-09-27', 'F', '509 Seventh Ave', NULL, 'St. Paul', 'MN', '55101', 'USA', '2020-06-03 11:50:00', '2024-01-20 15:30:00', 'Active', 'Premium', 21890.50, 64, 'Phone'),
# MAGIC (89, 'Roy', 'Powell', 'rpowell@email.com', '555-0189', '1991-11-04', 'M', '620 Eighth St', 'Suite 9', 'Norfolk', 'VA', '23502', 'USA', '2021-08-26 10:15:00', '2024-01-19 12:50:00', 'Active', 'Standard', 6120.00, 20, 'Email'),
# MAGIC (90, 'Anne', 'Long', 'along@email.com', '555-0190', '1984-02-11', 'F', '731 Ninth Rd', NULL, 'Toledo', 'OH', '43601', 'USA', '2020-03-18 13:05:00', '2024-01-21 10:25:00', 'Active', 'Premium', 18450.75, 53, 'SMS'),
# MAGIC (91, 'Louis', 'Patterson', 'lpatterson@email.com', '555-0191', '1993-06-28', 'M', '842 Tenth Dr', 'Apt 2A', 'Newark', 'NJ', '07102', 'USA', '2021-11-03 12:45:00', '2024-01-18 14:10:00', 'Active', 'Basic', 3450.25, 12, 'Email'),
# MAGIC (92, 'Marie', 'Hughes', 'mhughes@email.com', '555-0192', '1987-10-15', 'F', '953 Eleventh Ln', NULL, 'Jersey City', 'NJ', '07302', 'USA', '2020-09-20 09:30:00', '2024-01-22 11:40:00', 'Active', 'Standard', 10120.50, 35, 'Phone'),
# MAGIC (93, 'Bruce', 'Flores', 'bflores@email.com', '555-0193', '1995-12-02', 'M', '164 Twelfth Ct', 'Unit 5', 'Fort Wayne', 'IN', '46801', 'USA', '2022-01-21 11:20:00', '2024-01-17 16:55:00', 'Active', 'Premium', 15670.00, 42, 'Email'),
# MAGIC (94, 'Beverly', 'Washington', 'bwashington@email.com', '555-0194', '1980-04-19', 'F', '275 Maple Blvd', NULL, 'St. Petersburg', 'FL', '33701', 'USA', '2019-07-14 14:15:00', '2024-01-20 13:35:00', 'Active', 'Standard', 12890.75, 43, 'SMS'),
# MAGIC (95, 'Joe', 'Butler', 'jbutler@email.com', '555-0195', '1989-08-06', 'M', '386 Oak Ave', 'Apt 8C', 'Lincoln', 'NE', '68502', 'USA', '2020-11-12 10:50:00', '2024-01-19 09:20:00', 'Active', 'Premium', 17560.25, 49, 'Email'),
# MAGIC (96, 'Marilyn', 'Simmons', 'msimmons@email.com', '555-0196', '1992-10-23', 'F', '497 Pine St', NULL, 'Paradise', 'NV', '89101', 'USA', '2021-04-30 13:30:00', '2024-01-21 15:15:00', 'Active', 'Standard', 8230.50, 27, 'Phone'),
# MAGIC (97, 'Albert', 'Foster', 'afoster@email.com', '555-0197', '1982-01-10', 'M', '508 Elm Rd', 'Suite 1', 'Irvine', 'CA', '92602', 'USA', '2019-12-27 12:00:00', '2024-01-18 11:45:00', 'Suspended', 'Basic', 1450.00, 7, 'Email'),
# MAGIC (98, 'Denise', 'Russell', 'drussell@email.com', '555-0198', '1988-05-27', 'F', '619 Cedar Dr', NULL, 'Port St. Lucie', 'FL', '34952', 'USA', '2020-07-05 09:40:00', '2024-01-22 16:30:00', 'Active', 'Premium', 20120.75, 59, 'SMS'),
# MAGIC (99, 'Randy', 'Griffin', 'rgriffin@email.com', '555-0199', '1994-09-14', 'M', '730 Birch Ln', 'Apt 3B', 'Chesapeake', 'VA', '23321', 'USA', '2021-10-17 11:25:00', '2024-01-17 13:50:00', 'Active', 'Basic', 4230.25, 14, 'Email'),
# MAGIC (100, 'Jacqueline', 'Diaz', 'jdiaz@email.com', '555-0200', '1981-11-01', 'F', '841 Willow Ct', NULL, 'Garland', 'TX', '75041', 'USA', '2019-08-30 14:35:00', '2024-01-20 10:15:00', 'Active', 'Standard', 13450.50, 44, 'Phone'),
# MAGIC -- Records 101-150
# MAGIC (101, 'Keith', 'Hayes', 'khayes@email.com', '555-0201', '1987-03-18', 'M', '952 Spruce Blvd', 'Unit 6', 'Cape Coral', 'FL', '33904', 'USA', '2020-05-14 10:20:00', '2024-01-19 14:40:00', 'Active', 'Premium', 19230.00, 56, 'Email'),
# MAGIC (101, 'Keith', 'Hayes', 'khayes@email.com', '555-0201', '1987-03-18', 'M', '952 Spruce Blvd', 'Unit 6', 'Cape Coral', 'FL', '33904', 'USA', '2020-05-14 10:20:00', '2024-01-19 14:40:00', 'Active', 'Premium', 19230.00, 56, 'Email'),
# MAGIC (102, 'Cheryl', 'Myers', 'cmyers@email.com', '555-0202', '1993-07-05', 'F', '163 Pecan Ave', NULL, 'Springfield', 'MO', '65801', 'USA', '2021-01-22 12:55:00', '2024-01-21 12:10:00', 'Active', 'Standard', 7450.75, 25, 'SMS'),
# MAGIC (103, 'Philip', 'Ford', 'pford@email.com', '555-0203', '1980-11-22', 'M', '274 Alder St', 'Apt 5C', 'Salem', 'OR', '97301', 'USA', '2019-09-05 15:10:00', '2024-01-18 09:35:00', 'Inactive', 'Basic', 1780.25, 8, 'Email'),
# MAGIC (104, 'Martha', 'Hamilton', 'mhamilton@email.com', '555-0204', '1986-04-09', 'F', '385 Juniper Rd', NULL, 'Santa Rosa', 'CA', '95401', 'USA', '2020-10-31 11:40:00', '2024-01-22 15:25:00', 'Active', 'Premium', 22670.50, 69, 'Phone'),
# MAGIC (105, 'Terry', 'Graham', 'tgraham@email.com', '555-0205', '1991-08-26', 'M', '496 Laurel Dr', 'Suite 7', 'Grand Rapids', 'MI', '49501', 'USA', '2021-06-11 10:05:00', '2024-01-17 11:50:00', 'Active', 'Standard', 8560.00, 28, 'Email'),
# MAGIC (106, 'Kathryn', 'Sullivan', 'ksullivan@email.com', '555-0206', '1984-12-13', 'F', '507 Aspen Ln', NULL, 'Huntsville', 'AL', '35801', 'USA', '2020-02-26 13:25:00', '2024-01-20 16:40:00', 'Active', 'Premium', 18120.75, 52, 'SMS'),
# MAGIC (107, 'Gerald', 'Wallace', 'gwallace@email.com', '555-0207', '1992-02-20', 'M', '618 Cottonwood Ct', 'Apt 9D', 'Grand Prairie', 'TX', '75050', 'USA', '2021-09-02 12:40:00', '2024-01-19 13:15:00', 'Active', 'Basic', 3890.25, 13, 'Email'),
# MAGIC (108, 'Theresa', 'Woods', 'twoods@email.com', '555-0208', '1988-06-07', 'F', '729 Mesquite Blvd', NULL, 'Brownsville', 'TX', '78520', 'USA', '2020-08-18 09:50:00', '2024-01-21 10:30:00', 'Active', 'Standard', 11890.50, 40, 'Phone'),
# MAGIC (109, 'Scott', 'West', 'swest@email.com', '555-0209', '1995-10-24', 'M', '840 Sequoia Ave', 'Unit 2', 'McKinney', 'TX', '75069', 'USA', '2021-12-28 11:15:00', '2024-01-18 14:55:00', 'Active', 'Premium', 16340.00, 44, 'Email'),
# MAGIC (110, 'Sara', 'Cole', 'scole@email.com', '555-0210', '1981-03-11', 'F', '951 Buckeye St', NULL, 'Des Moines', 'IA', '50301', 'USA', '2019-06-19 14:30:00', '2024-01-22 12:20:00', 'Active', 'Standard', 12560.75, 42, 'SMS'),
# MAGIC (111, 'Willie', 'Reynolds', 'wreynolds@email.com', '555-0211', '1989-09-28', 'M', '162 Ironwood Rd', 'Apt 4D', 'Overland Park', 'KS', '66204', 'USA', '2020-11-07 10:35:00', '2024-01-17 09:45:00', 'Active', 'Premium', 20890.25, 61, 'Email'),
# MAGIC (112, 'Janice', 'Fisher', 'jfisher@email.com', '555-0212', '1994-01-15', 'F', '273 Maplewood Dr', NULL, 'Montgomery', 'AL', '36101', 'USA', '2021-03-16 13:50:00', '2024-01-20 15:35:00', 'Active', 'Standard', 6780.50, 22, 'Phone'),
# MAGIC (113, 'Billy', 'Ellis', 'bellis@email.com', '555-0213', '1983-05-02', 'M', '384 Pinewood Ln', 'Suite 5', 'Columbus', 'GA', '31901', 'USA', '2020-04-23 12:10:00', '2024-01-19 11:05:00', 'Suspended', 'Basic', 1230.00, 6, 'Email'),
# MAGIC (114, 'Julia', 'Marshall', 'jmarshall@email.com', '555-0214', '1990-11-19', 'F', '495 Oakmont Ct', NULL, 'Moreno Valley', 'CA', '92551', 'USA', '2021-07-09 09:25:00', '2024-01-21 13:50:00', 'Active', 'Premium', 19560.75, 57, 'SMS'),
# MAGIC (115, 'Johnny', 'Owens', 'jowens@email.com', '555-0215', '1986-07-06', 'M', '506 Cedarbrook Blvd', 'Apt 1B', 'Huntington Beach', 'CA', '92646', 'USA', '2020-09-15 11:45:00', '2024-01-18 16:25:00', 'Active', 'Standard', 9670.25, 33, 'Email'),
# MAGIC (116, 'Kathy', 'Payne', 'kpayne@email.com', '555-0216', '1992-12-23', 'F', '617 Elmhurst Ave', NULL, 'Fontana', 'CA', '92335', 'USA', '2021-10-06 10:30:00', '2024-01-22 10:15:00', 'Active', 'Premium', 17890.50, 50, 'Phone'),
# MAGIC (117, 'Roger', 'Goodwin', 'rgoodwin@email.com', '555-0217', '1979-04-10', 'M', '728 Brookside St', 'Unit 8', 'Fayetteville', 'NC', '28301', 'USA', '2019-11-22 15:20:00', '2024-01-17 12:40:00', 'Inactive', 'Basic', 1560.00, 7, 'Email'),
# MAGIC (118, 'Heather', 'Watkins', 'hwatkins@email.com', '555-0218', '1985-08-27', 'F', '839 Riverside Rd', NULL, 'Sioux Falls', 'SD', '57101', 'USA', '2020-06-29 12:55:00', '2024-01-20 14:30:00', 'Active', 'Standard', 10780.75, 37, 'SMS'),
# MAGIC (119, 'Bobby', 'Olson', 'bolson@email.com', '555-0219', '1993-10-14', 'M', '950 Lakeview Dr', 'Apt 6B', 'Tacoma', 'WA', '98401', 'USA', '2021-05-21 09:40:00', '2024-01-19 11:20:00', 'Active', 'Basic', 4560.25, 15, 'Email'),
# MAGIC (120, 'Teresa', 'Lawson', 'tlawson@email.com', '555-0220', '1987-02-01', '   F', '161 Hillside Ln', NULL, 'Shreveport', 'LA', '71101', 'USA', '2020-12-12 14:05:00', '2024-01-21 16:45:00', 'Active', 'Premium', 21340.50, 63, 'Phone'),
# MAGIC (121, 'Jerry', 'Carr', 'jcarr@email.com', '555-0221', '1991-06-18', 'M', '272 Mountain Ct', 'Suite 3', 'Knoxville', 'TN', '37901', 'USA', '2021-08-14 11:50:00', '2024-01-18 09:30:00', 'Active', 'Standard', 8340.00, 27, 'Email'),
# MAGIC (122, 'Christina', 'Boyd', 'cboyd@email.com', '555-0222', '1984-10-05', 'F', '383 Forest Blvd', NULL, 'Augusta', 'GA', '30901', 'USA', '2020-03-27 10:15:00', '2024-01-22 13:55:00', 'Active', 'Premium', 18670.75, 54, 'SMS'),
# MAGIC (123, 'Jordan', 'Murray', 'jmurray@email.com', '555-0223', '1996-12-22', 'M', '494 Valley Ave', 'Apt 7A', 'Mobile', 'AL', '36601', 'USA', '2022-02-18 13:30:00', '2024-01-17 15:10:00', 'Active', 'Basic', 2120.25, 7, 'Email'),
# MAGIC (124, 'Doris', 'Crawford', 'dcrawford@email.com', '555-0224', '1982-04-19', 'F', '505 Garden St', NULL, 'Little Rock', 'AR', '72201', 'USA', '2020-01-05 12:20:00', '2024-01-20 10:50:00', 'Active', 'Standard', 13230.50, 45, 'Phone'),
# MAGIC (125, 'Billy', 'Black', 'bblack@email.com', '555-0225', '1988-08-06', 'M', '616 Spring Rd', 'Unit 4', 'Amarillo', 'TX', '79101', 'USA', '2020-07-17 09:05:00', '2024-01-19 12:35:00', 'Active', 'Premium', 19780.00, 58, 'Email'),
# MAGIC (126, 'Christina', 'Bradley', 'cbradley@email.com', '555-0226', '1993-11-23', 'F', '727 Harbor Dr', NULL, 'Eugene', 'OR', '97401', 'USA', '2021-09-25 14:45:00', '2024-01-21 15:20:00', 'Active', 'Standard', 7120.75, 24, 'SMS'),
# MAGIC (127, 'Joe', 'Spencer', 'jspencer@email.com', '555-0227', '1980-03-10', 'M', '838 Bay Ln', 'Apt 2C', 'Glendale', 'AZ', '85301', 'USA', '2019-10-28 11:30:00', '2024-01-18 16:05:00', 'Inactive', 'Basic', 1890.25, 8, 'Email'),
# MAGIC (128, 'Kathleen', 'Lane', 'klane@email.com', '555-0228', '1986-07-27', 'F', '949 Beach Ct', NULL, 'Huntington', 'NY', '11743', 'USA', '2020-05-09 10:50:00', '2024-01-22 11:35:00', 'Active', 'Premium', 22340.50, 68, 'Phone'),
# MAGIC (129, 'Jesse', 'Mills', 'jmills@email.com', '555-0229', '1992-09-14', 'M', '160 Ocean Blvd', 'Suite 9', 'Salinas', 'CA', '93901', 'USA', '2021-04-03 13:15:00', '2024-01-17 13:50:00', 'Active', 'Basic', 3670.00, 12, 'Email'),
# MAGIC (130, 'Joan', 'Dean', 'jdean@email.com', '555-0230', '1985-01-21', 'F', '271 River Ave', NULL, 'Tallahassee', 'FL', '32301', 'USA', '2020-11-20 12:35:00', '2024-01-20 09:25:00', 'Active', 'Standard', 11450.75, 39, 'SMS'),
# MAGIC (131, 'Eugene', 'Oliver', 'eoliver@email.com', '555-0231', '1991-05-08', 'M', '382 Park St', 'Apt 5A', 'Rockford', 'IL', '61101', 'USA', '2021-07-27 09:55:00', '2024-01-19 14:15:00', 'Active', 'Premium', 17230.25, 48, 'Email'),
# MAGIC (132, 'Rose', 'Knight', 'rknight@email.com', '555-0232', '1984-09-25', 'F', '493 Lake Rd', NULL, 'Vancouver', 'WA', '98660', 'USA', '2020-04-12 11:20:00', '2024-01-21 12:50:00', 'Active', 'Standard', 8890.50, 29, 'Phone'),
# MAGIC (133, 'Ralph', 'Robertson', 'rrobertson@email.com', '555-0233', '1993-12-02', 'M', '504 Hill Dr', 'Unit 1', 'Ontario', 'CA', '91761', 'USA', '2021-10-14 14:10:00', '2024-01-18 10:40:00', 'Active', 'Premium', 16780.00, 46, 'Email'),
# MAGIC (134, 'Judy', 'Burton', 'jburton@email.com', '555-0234', '1987-06-19', 'F', '615 Grove Ln', NULL, 'Chattanooga', 'TN', '37401', 'USA', '2020-09-08 10:25:00', '2024-01-22 15:55:00', 'Active', 'Standard', 10230.75, 35, 'SMS')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Create customers table
# MAGIC CREATE OR REPLACE TABLE testschema.tennessee_customers (
# MAGIC     customer_id BIGINT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     date_of_birth DATE,
# MAGIC     gender STRING,
# MAGIC     address_line1 STRING,
# MAGIC     address_line2 STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     zip_code STRING,
# MAGIC     country STRING,
# MAGIC     registration_date TIMESTAMP,
# MAGIC     last_login_date TIMESTAMP,
# MAGIC     account_status STRING,
# MAGIC     customer_segment STRING,
# MAGIC     lifetime_value DECIMAL(10,2),
# MAGIC     total_orders INT,
# MAGIC     preferred_contact_method STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC insert into testschema.tennessee_customers
# MAGIC select * from testschema.customers
# MAGIC where state = 'TN';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%sql
select * from testschema.customers
where firstname = 'John'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
