from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MongoDBIntegration") \
        .config("spark.mongodb.input.uri", "mongodb://localhost/mydb.questions") \
        .config("spark.mongodb.output.uri", "mongodb://localhost/mydb.questions") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config('spark.sql.legacy.timeParserPolicy', 'LEGACY') \
        .config("spark.dynamicAllocation.executorIdleTimeout", "30s") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.default.parallelism", "16") \
        .enableHiveSupport() \
        .getOrCreate()

    # Define the schema for your DataFrame
    question_schema = StructType([
        StructField("Id", IntegerType(), True),
        StructField("OwnerUserId", StringType(), True),  # Keep OwnerUserId as StringType
        StructField("CreationDate", StringType(), True),  # Keep CreationDate as StringType
        StructField("ClosedDate", StringType(), True),    # Keep ClosedDate as StringType
        StructField("Score", IntegerType(), True),
        StructField("Title", StringType(), True),
        StructField("Body", StringType(), True)
    ])

    # Read data from mydb.questions
    questions_df = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .schema(question_schema) \
        .load()

    # Convert CreationDate and ClosedDate to DateType for mydb.questions
    questions_df = questions_df.withColumn("CreationDate", to_date(col("CreationDate"), "yyyy-MM-dd"))
    questions_df = questions_df.withColumn("ClosedDate", to_date(col("ClosedDate"), "yyyy-MM-dd"))

    # Read data from mydb.answers
    answer_schema = StructType([
        StructField("Id", IntegerType(), True),  # Renamed to "AnswerId"
        StructField("OwnerUserId", StringType(), True),
        StructField("CreationDate", StringType(), True),  # Keep it as StringType for now
        StructField("ParentId", StringType(), True),  # Renamed to "AnswerScore"
        StructField("Score", IntegerType(), True),  # Renamed to "QuestionId"
        StructField("Body", StringType(), True)  # Renamed to "AnswerBody"
    ])
    answers_df = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .schema(answer_schema) \
        .option("uri", "mongodb://localhost/mydb.answers") \
        .load()

    # Convert AnswerCreationDate to DateType for mydb.answers
    answers_df = answers_df.withColumn("CreationDate", (to_date(col("CreationDate"), "yyyy-MM-dd")))
    '''
    spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("USE MY_DB")
    answers_df.coalesce(1).write \
        .bucketBy(10, "Id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.answers")

    questions_df.coalesce(1).write \
        .bucketBy(10, "Id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.question")
    # Refresh the tables after saving
    spark.sql("REFRESH TABLE MY_DB")
    spark.sql("REFRESH TABLE MY_DB")
    '''
    # Perform the join between questions and answers based on the "QuestionId" and "QuestionId" columns
    df_answers_sql = spark.read.table("MY_DB.answers")
    df_questions_sql = spark.read.table("MY_DB.question")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    joined_df = df_answers_sql.join(df_questions_sql, df_questions_sql["Id"] == df_answers_sql["ParentId"], "inner")
    joined_df.show()
    # Continue with your processing as needed

    # Stop the Spark session
    spark.stop()
