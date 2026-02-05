-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "cc9c8a10-7f1d-4d7b-9ce1-ea7923bda6d6",
-- META       "default_lakehouse_name": "demolakehouse",
-- META       "default_lakehouse_workspace_id": "85cf6401-8eeb-419d-9a64-acfe3164d82d",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "cc9c8a10-7f1d-4d7b-9ce1-ea7923bda6d6"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "d930d8c5-c0a2-4e06-bac5-074b4e1eae02",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "d930d8c5-c0a2-4e06-bac5-074b4e1eae02",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!

# Mount external lakehouse  "demolakehouse" into specified local path.
# Note: NotebookUtils is only supported on runtime v1.2 and above. If you are using runtime v1.1, please use mssparkutils instead.
notebookutils.fs.mount("abfss://demofabric@onelake.dfs.fabric.microsoft.com/demolakehouse.Lakehouse", "/lakehouse/demolakehouse")

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
# Load image
image = mpimg.imread(notebookutils.fs.getMountPath('/lakehouse/demolakehouse/Files/Joe Rodriguez CV.png'))
# Let the axes disappear
plt.axis('off')
# Plot image in the output
image_plot = plt.imshow(image)


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

df = spark.sql("SELECT * FROM demolakehouse.testschema.customers LIMIT 1000")
display(df)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
