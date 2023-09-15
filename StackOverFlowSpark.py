from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, explode, split, count, regexp_extract, expr, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MongoDBIntegration") \
        .config("spark.mongodb.input.uri", "mongodb://localhost/mydb.questions") \
        .config("spark.mongodb.output.uri", "mongodb://localhost/mydb.questions") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config('spark.sql.legacy.timeParserPolicy', 'LEGACY') \
        .getOrCreate()

    # Define the schema for your DataFrame
    schema = StructType([
        StructField("Id", IntegerType(), True),
        StructField("OwnerUserId", StringType(), True),  # Keep OwnerUserId as StringType
        StructField("CreationDate", StringType(), True),  # Keep CreationDate as StringType
        StructField("ClosedDate", StringType(), True),  # Keep ClosedDate as StringType
        StructField("Score", IntegerType(), True),
        StructField("Title", StringType(), True),
        StructField("Body", StringType(), True)
    ])

    # Read data from MongoDB into a Spark DataFrame
    df = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .schema(schema) \
        .load()

    # Convert CreationDate và ClosedDate to DateType()
    df = df.withColumn("CreationDate", to_date(col("CreationDate"), "yyyy-MM-dd"))
    df = df.withColumn("ClosedDate", to_date(col("ClosedDate"), "yyyy-MM-dd"))

    # Define the words you want to extract
    words_to_extract = ["Java", "Python", "C++", "C#", "Go", "Ruby", "JavaScript", "PHP", "HTML", "CSS", "SQL"]

    # Split the "Body" column into words, explode the resulting array, and filter the words of interest
    extracted_words = df.select("Id", "Body") \
        .withColumn("word", explode(split(df["Body"], " "))) \
        .filter(col("word").isin(*words_to_extract))

    # Count the occurrences of each extracted word
    word_counts = extracted_words.groupBy("word").agg(count("*").alias("count"))

    ######## Show the word counts
    word_counts.show()

    # Define the regex pattern to extract domains
    regex_pattern = r'https?://([^/]+)'

    # Extract domains using regex and aggregation
    df_web = df.withColumn("Domain", regexp_extract(col("Body"), regex_pattern, 1))

    # Filter out rows with empty "Domain" values
    df_web = df_web.filter(col("Domain") != "")

    # Group by domain and count occurrences
    domain_counts = df_web.groupBy("Domain") \
        .agg(expr("count(*) as Count")) \
        .orderBy(col("Count").desc()) \
        .limit(20)

    ########## Show the top 20 domains
    domain_counts.show()

    # Filter out rows with 'NA' values in the "OwnerUserId" column
    df_window = df.filter(df["OwnerUserId"] != 'NA')

    # Cast "OwnerUserId" to IntegerType
    df_window = df_window.withColumn("OwnerUserId", df["OwnerUserId"].cast(IntegerType()))

    # Filter out rows with null values in the "OwnerUserId" column
    df_window = df_window.filter(df["OwnerUserId"].isNotNull())

    # Define the window specification
    running_total_window = Window.partitionBy("OwnerUserId") \
        .orderBy("CreationDate") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Compute the running total of "Score" for each user
    df_window = df_window.withColumn("TotalScore", sum("Score").over(running_total_window))

    # Order the DataFrame by "TotalScore" column from largest to smallest
    df_window = df_window.select("OwnerUserId", "CreationDate", "TotalScore").orderBy(col("TotalScore").desc())

    ########### Show window of user's score
    df_window.show()

    # Stop the Spark session
    spark.stop()
