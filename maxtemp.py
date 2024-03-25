from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, when, lit
from tabulate import tabulate

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MaxTemperatures") \
    .getOrCreate()

# Define the file path
file_path = "temp1800.csv"

# Read CSV into a DataFrame, considering only the first four columns
df = spark.read.csv(file_path, header=False).select("_c0", "_c1", "_c2", "_c3")

# Define schema
schema = ["station_id", "date", "element", "temperature"]

# Assign schema to DataFrame
df = df.toDF(*schema)

# Convert temperature to Celsius
df = df.withColumn("temperature_celsius", col("temperature").cast("float") / 10)

# Extract month from date
df = df.withColumn("month", col("date").substr(5, 2).cast("int"))

# Filter for TMAX records
df = df.filter(col("element") == "TMAX")

# Dictionary to hold maximum temperatures for each month
max_temperatures = {"Jan": {"Temperature": float("-inf"), "Date": "", "Station Name": ""},
                    "Apr": {"Temperature": float("-inf"), "Date": "", "Station Name": ""},
                    "Jul": {"Temperature": float("-inf"), "Date": "", "Station Name": ""},
                    "Oct": {"Temperature": float("-inf"), "Date": "", "Station Name": ""}}

# Iterate over months to find maximum temperatures
for month in ["Jan", "Apr", "Jul", "Oct"]:
    month_num = {"Jan": 1, "Apr": 4, "Jul": 7, "Oct": 10}[month]
    temp_df = df.filter(col("month") == month_num)
    max_temp = temp_df.agg(spark_max("temperature_celsius")).collect()[0][0]
    max_temp_row = temp_df.filter(col("temperature_celsius") == max_temp).first()

    # Update maximum temperature for each month
    max_temperatures[month]["Temperature"] = max_temp
    max_temperatures[month]["Date"] = max_temp_row["date"]
    max_temperatures[month]["Station Name"] = max_temp_row["station_id"]

# Prepare data for printing
data = [["Month"] + list(max_temperatures.keys())]
for param in ["Temperature", "Date", "Station Name"]:
    row = [param]
    for month in max_temperatures:
        row.append(max_temperatures[month][param])
    data.append(row)

# Print the result using tabulate
print(tabulate(data, tablefmt="grid"))

# Stop SparkSession
spark.stop()
