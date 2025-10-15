from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import Row

# Assuming this is the function you are testing
from src.sales_transformations import clean_sales

def test_no_negative_or_null_amounts(spark):
    """
    Verifies that the clean_sales function removes or corrects rows
    where 'total_amount' is negative or null.
    """
    # Define a clear, consistent schema (best practice)
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("product_category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price_per_unit", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

    # Create data using Row objects, which is clean and explicit
    input_data = [
        Row(transaction_id=1, date='2023-01-01', customer_id=101, gender='Male', age=25, product_category='Electronics', quantity=1, price_per_unit=299.99, total_amount=299.99),
        Row(transaction_id=2, date='2023-01-02', customer_id=102, gender='Female', age=30, product_category='Clothing', quantity=2, price_per_unit=49.99, total_amount=-99.98),  # Invalid: negative amount
        Row(transaction_id=3, date='2023-01-03', customer_id=103, gender='Female', age=22, product_category='Books', quantity=1, price_per_unit=19.99, total_amount=None),       # Invalid: null amount
        Row(transaction_id=4, date='2023-01-04', customer_id=104, gender='Male', age=28, product_category='Home', quantity=1, price_per_unit=89.99, total_amount=89.99)
    ]
    
    # Create the DataFrame with the explicit schema
    df = spark.createDataFrame(input_data, schema)
    
    # Run the function to be tested
    cleaned_df = clean_sales(df)
    
    # Assert that no invalid rows remain
    invalid_rows = cleaned_df.filter("total_amount < 0 OR total_amount IS NULL").count()
    assert invalid_rows == 0, "Test failed: Found rows with negative or null total_amount after cleaning."