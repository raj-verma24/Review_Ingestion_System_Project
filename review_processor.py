import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, length, avg, count, min, max, expr, round as spark_round
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, FloatType
from datetime import datetime, timedelta

# Define a schema for the reviews
review_schema = StructType([
    StructField("restaurantId", IntegerType(), True),
    StructField("reviewId", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("publishedAt", StringType(), True)
])

# Load inappropriate words from a file
def load_inappropriate_words(file_path):
    with open(file_path, 'r') as f:
        return set([line.strip() for line in f])

# UDF to count inappropriate words
def count_inappropriate_words(text, inappropriate_words):
    if text is None:
        return 0
    words = text.split()
    return sum(1 for word in words if word.lower() in inappropriate_words)

# UDF to replace inappropriate words
def replace_inappropriate_words(text, inappropriate_words):
    if text is None:
        return text
    words = text.split()
    return " ".join(["****" if word.lower() in inappropriate_words else word for word in words])

# Register UDFs
def register_udfs(spark, inappropriate_words):
    count_udf = udf(lambda text: count_inappropriate_words(text, inappropriate_words), IntegerType())
    replace_udf = udf(lambda text: replace_inappropriate_words(text, inappropriate_words), StringType())
    return count_udf, replace_udf

# Main function to process reviews
def process_reviews(input_file, inappropriate_words_file, output_file, discarded_file, aggregation_file):
    # Initialize Spark session
    spark = SparkSession.builder.appName("ReviewProcessor").getOrCreate()
    
    # Load inappropriate words
    inappropriate_words = load_inappropriate_words(inappropriate_words_file)
    
    # Load the reviews data into a DataFrame
    df = spark.read.schema(review_schema).json(input_file)

    # Register UDFs for counting and replacing inappropriate words
    count_words_udf, replace_words_udf = register_udfs(spark, inappropriate_words)
    
    # Filter invalid records
    df_filtered = df.filter(
        col("restaurantId").isNotNull() & 
        col("reviewId").isNotNull() & 
        col("text").isNotNull() & 
        col("rating").isNotNull() & 
        col("publishedAt").isNotNull()
    )
    
    # Convert publishedAt to timestamp and filter old records
    three_years_ago = (datetime.now() - timedelta(days=3*365)).isoformat()
    df_filtered = df_filtered.filter(col("publishedAt") >= three_years_ago)
    
    #Filtering reviews which have valid ratings between 0 to 10
    df_filtered=df_filtered.filter(col("rating").between(0, 10))
    
    # Count inappropriate words
    df_filtered = df_filtered.withColumn("inappropriate_count", count_words_udf(col("text")))

    # Filter reviews with inappropriate words count exceeding 20% of total words
    df_filtered = df_filtered.filter(
        (col("inappropriate_count") / length(col("text").cast(StringType()))) <= 0.2
    )
    
    # Replace inappropriate words in valid reviews
    df_filtered = df_filtered.withColumn("cleaned_text", replace_words_udf(col("text")))
    
    # Cache the filtered DataFrame
    df_filtered.cache()

    # Write valid reviews directly as JSONL
    df_filtered.select("restaurantId", "reviewId", "cleaned_text", "rating", "publishedAt") \
    .withColumnRenamed("cleaned_text", "text") \
    .coalesce(1) \
    .write.json(output_file, mode='overwrite')

    # Discarded reviews: Perform subtract operation
    df_discarded = df.subtract(df_filtered.select("restaurantId", "reviewId", "text", "rating", "publishedAt"))
    
    # Write discarded reviews as a single JSONL file
    df_discarded.coalesce(1).write.json(discarded_file, mode='overwrite')

    # Perform aggregation calculations
    current_time = datetime.now().isoformat()
    df_filtered = df_filtered.withColumn("days_since_review", expr(f"datediff('{current_time}', publishedAt)"))
    
    aggregation_df = df_filtered.groupBy("restaurantId").agg(
        count("reviewId").alias("reviewCount"),
        spark_round(avg("rating"), 2).alias("averageRating"),
        spark_round(avg(length(col("text"))), 2).alias("averageReviewLength"),
        min("days_since_review").alias("oldest"),
        max("days_since_review").alias("newest"),
        spark_round(avg("days_since_review"), 2).alias("average")
    )

   # Write aggregation results as a single JSONL file
    aggregation_df.coalesce(1).write.json(aggregation_file, mode='overwrite')

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    import argparse
    # Parsing command line arguments
    arg_parser = argparse.ArgumentParser(description='Process and aggregate restaurant reviews.')
    arg_parser.add_argument('--input', required=True, help='Path to input JSONL file')
    arg_parser.add_argument('--inappropriate_words', required=True, help='Path to inappropriate words text file')
    arg_parser.add_argument('--output', required=True, help='Path to output valid reviews JSONL file')
    arg_parser.add_argument('--discarded', required=True, help='Path to discarded reviews JSONL file')
    arg_parser.add_argument('--aggregations', required=True, help='Path to output aggregation JSONL file')

    args = arg_parser.parse_args()

    # Process the reviews
    process_reviews(args.input, args.inappropriate_words, args.output, args.discarded, args.aggregations)
