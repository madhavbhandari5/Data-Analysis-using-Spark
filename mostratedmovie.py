from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MostPopularMovies") \
    .getOrCreate()

# Read the input data (u.data)
data = spark.read.option("delimiter", "\t").csv("u.data", inferSchema=True).toDF("user_id", "movie_id", "rating", "timestamp")

# Group by movie_id and count the number of ratings for each movie
movie_ratings_count = data.groupBy("movie_id").agg(count("rating").alias("num_ratings"))

# Filter movies that are rated at least 30 times
popular_movies = movie_ratings_count.filter(col("num_ratings") >= 30)

# Sort by total ratings in descending order and select top 5
top_five_movies = popular_movies.sort(desc("num_ratings")).limit(5)

# Show the result
top_five_movies.show(truncate=False)

# Stop SparkSession
spark.stop()
