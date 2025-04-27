# Databricks notebook source
# MAGIC %md
# MAGIC # Load Data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## MOUNT THE BLOB STORAGE 

# COMMAND ----------

mount_point = "/mnt/trends"

if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source="wasbs://containername@storageaccountname.blob.core.windows.net/",
        mount_point=mount_point,
        extra_configs={
            "fs.azure.account.key.containername.blob.core.windows.net": "accesskey"
        }
    )
    print(f" Mounted: {mount_point}")
else:
    print(f" Already mounted: {mount_point}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## DOWNLOAD THE DATASET FROM KAGGLE

# COMMAND ----------

# import pandas as pd

# COMMAND ----------

# %pip install kagglehub

# COMMAND ----------

# import kagglehub
# path = kagglehub.dataset_download("grouplens/movielens-20m-dataset")
# print("Path to dataset files:", path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## COPY THE FILES TO BLOB

# COMMAND ----------

# # Copy Local file to DBFS
# dbutils.fs.cp("file:/root/.cache/kagglehub/datasets/grouplens/movielens-20m-dataset/versions/1/rating.csv", "dbfs:/FileStore/movielens/rating.csv")
# dbutils.fs.cp("file:/root/.cache/kagglehub/datasets/grouplens/movielens-20m-dataset/versions/1/genome_scores.csv", "dbfs:/FileStore/movielens/genome_scores.csv")
# dbutils.fs.cp("file:/root/.cache/kagglehub/datasets/grouplens/movielens-20m-dataset/versions/1/genome_tags.csv", "dbfs:/FileStore/movielens/genome_tags.csv")
# dbutils.fs.cp("file:/root/.cache/kagglehub/datasets/grouplens/movielens-20m-dataset/versions/1/link.csv", "dbfs:/FileStore/movielens/link.csv")
# dbutils.fs.cp("file:/root/.cache/kagglehub/datasets/grouplens/movielens-20m-dataset/versions/1/movie.csv", "dbfs:/FileStore/movielens/movie.csv")
# dbutils.fs.cp("file:/root/.cache/kagglehub/datasets/grouplens/movielens-20m-dataset/versions/1/tag.csv", "dbfs:/FileStore/movielens/tag.csv")

# COMMAND ----------

# # Move files to /mnt/trends/dataset
# dbutils.fs.mv("dbfs:/FileStore/movielens/rating.csv", "dbfs:/mnt/trends/dataset/rating.csv")
# dbutils.fs.mv("dbfs:/FileStore/movielens/genome_scores.csv", "dbfs:/mnt/trends/dataset/genome_scores.csv")
# dbutils.fs.mv("dbfs:/FileStore/movielens/genome_tags.csv", "dbfs:/mnt/trends/dataset/genome_tags.csv")
# dbutils.fs.mv("dbfs:/FileStore/movielens/link.csv", "dbfs:/mnt/trends/dataset/link.csv")
# dbutils.fs.mv("dbfs:/FileStore/movielens/movie.csv", "dbfs:/mnt/trends/dataset/movie.csv")
# dbutils.fs.mv("dbfs:/FileStore/movielens/tag.csv", "dbfs:/mnt/trends/dataset/tag.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### WRITING INITIAL DATAFRAME TO SQL

# COMMAND ----------

# # 1. Read movie.csv into a DataFrame
# movie_df = spark.read.option("header", True).csv("/mnt/trends/dataset/movie.csv")

# # 2. Define Azure SQL JDBC connection
# jdbc_url = "jdbc:sqlserver://sqlserver.database.windows.net:1433;" \
#            "database=trends;" \
#            "user=username;" \
#            "password=sqlpassword;" \
#            "encrypt=true;" \
#            "trustServerCertificate=false;" \
#            "hostNameInCertificate=*.database.windows.net;" \
#            "loginTimeout=30;"

# connection_properties = {
#     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# }

# # 3. Write to Azure SQL table `movie_data`
# movie_df.write \
#     .mode("overwrite") \
#     .jdbc(url=jdbc_url, table="movie_data", properties=connection_properties)

# COMMAND ----------

# ratings_df.repartition(10).write \
#     .mode("overwrite") \
#     .option("batchsize", 10000) \
#     .option("truncate", True) \
#     .jdbc(url=jdbc_url, table="ratings_data", properties=connection_properties)

# COMMAND ----------

# MAGIC %md
# MAGIC ### NOTE WE ARE CONVERTING RATINGS TO DELTA FORMAT HERE

# COMMAND ----------

# # Read data to SparkDF
# ratings_df = spark.read.options(header=True, inferSchema=True).csv("dbfs:/FileStore/movielens/rating.csv")
# ratings_df.write.format("delta").mode("overwrite").save("/mnt/trends/delta/ratings") 
# genome_scores_df = spark.read.options(header=True, inferSchema=True).csv("dbfs:/FileStore/movielens/genome_scores.csv")
# genome_tags_df = spark.read.options(header=True, inferSchema=True).csv("dbfs:/FileStore/movielens/genome_tags.csv")
# link_df = spark.read.options(header=True, inferSchema=True).csv("dbfs:/FileStore/movielens/link.csv")
# movie_df = spark.read.options(header=True, inferSchema=True).csv("dbfs:/FileStore/movielens/movie.csv")
# tag_df = spark.read.options(header=True, inferSchema=True).csv("dbfs:/FileStore/movielens/tag.csv")

# COMMAND ----------

# ratings_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## INITIAL MODEL RUN

# COMMAND ----------

# from pyspark.ml.recommendation import ALS
# from pyspark.sql.functions import explode, col

# ratings_df = spark.read.format("delta").load("/mnt/trends/delta/ratings")
# movies_df = spark.read.csv("/mnt/trends/dataset/movie.csv", header=True, inferSchema=True)

# als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop", nonnegative=True)
# model = als.fit(ratings_df)


# user_recs = model.recommendForAllUsers(10)
# flat_recs = user_recs.select(col("userId"), explode("recommendations").alias("rec")).select(
#     "userId", col("rec.movieId").alias("movieId"), col("rec.rating").alias("score")
# )
# final_recs = flat_recs.join(movies_df, on="movieId")
# final_recs.write.mode("overwrite").parquet("/mnt/blob/recommendations/user_recs.parquet")

# COMMAND ----------

# final_recs.show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK STREAMING SESSION

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, DoubleType, TimestampType

rating_schema = StructType() \
    .add("userId", IntegerType()) \
    .add("movieId", IntegerType()) \
    .add("rating", DoubleType()) \
    .add("timestamp", TimestampType())

stream_path = "/mnt/trends/ratings_stream/"
delta_path = "/mnt/trends/delta/ratings"

# JDBC config for Azure SQL DB
jdbc_url = "jdbc:sqlserver://sqlserver.database.windows.net:1433;" \
           "database=trends;" \
           "user=username;" \
           "password=sqlpassword;" \
           "encrypt=true;" \
           "trustServerCertificate=false;" \
           "hostNameInCertificate=*.database.windows.net;" \
           "loginTimeout=30;"

connection_properties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Define the foreachBatch function
def write_to_sql(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .jdbc(url=jdbc_url, table="ratings_data", properties=connection_properties)

# Start the streaming query
(
  spark.readStream
    .schema(rating_schema)
    .option("header", True)
    .csv(stream_path)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/trends/delta/checkpoints/ratings")
    .foreachBatch(write_to_sql)  # 👈 Add this line
    .start(delta_path)
)
