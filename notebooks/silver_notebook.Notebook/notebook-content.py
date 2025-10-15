# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e9a81e65-8046-4f7f-b3c8-0ffdf3bcd943",
# META       "default_lakehouse_name": "Sales_Lakehouse",
# META       "default_lakehouse_workspace_id": "ff9fc356-755f-447d-8c6b-ccbd95cc0f83",
# META       "known_lakehouses": [
# META         {
# META           "id": "e9a81e65-8046-4f7f-b3c8-0ffdf3bcd943"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from sales_transformations import clean_sales

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from src.transformations.sales_transformations import clean_sales
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.sql("SELECT * FROM Sales_Lakehouse.bronze.sales")
silver_df = clean_sales(bronze_df)
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver.silver_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import functions as F

# # Parse Date, map Gender, rename columns, and add Month/Year/Quarter
# df = (
#     df
#     # If your date is a string, specify the format if needed, e.g. 'MM/dd/yyyy'
#     .withColumn("Date", F.to_date(F.col("Date")))    # or F.to_date("Date", "MM/dd/yyyy")
#     .withColumn("Gender", F.when(F.col("Gender") == "Male", F.lit("M")).otherwise(F.lit("F")))
#     .withColumn("Month", F.month("Date"))
#     .withColumn("Year", F.year("Date"))
#     .withColumn("Quarter", F.quarter("Date"))
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# bins = [18, 24, 34, 44, 54, 64]
# labels = [
#     "Young Adults (18-24)",
#     "Adults (25-34)",
#     "Mature Adults (35-44)",
#     "Middle-aged (45-54)",
#     "Older Adults (55-64)"
# ]

# df['AgeGroup'] = pd.cut(df['Age'], bins=bins, labels=labels, right=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ax = sns.countplot(data=df, x="AgeGroup")
# plt.xlabel("Age Groups")
# plt.ylabel("Count")
# plt.title("Age Distribution by Meaningful Groups")
# plt.xticks(rotation=30) 
# plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").saveAsTable("silver.silver_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
