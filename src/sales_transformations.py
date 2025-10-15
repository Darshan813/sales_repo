from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_sales(df: DataFrame) -> DataFrame:

    # 1. Rename columns to be consistent (e.g., "Total Amount" -> "total_amount")
    renamed_df = df
    for column in df.columns:
        new_col_name = column.replace(' ', '_').lower()
        renamed_df = renamed_df.withColumnRenamed(column, new_col_name)

    # 2. Perform the transformations
    cleaned_df = (renamed_df
        # Standardize gender column
        .withColumn("gender",
            F.when(F.col("gender") == "Male", "M")
             .when(F.col("gender") == "Female", "F")
             .otherwise(F.col("gender"))
        )
       
        .withColumn("total_amount",
            F.round(F.col("quantity") * F.col("price_per_unit"), 2)
        )
        .na.drop(subset=["total_amount"])
        .filter(F.col("total_amount") > 0)
    )

    return cleaned_df