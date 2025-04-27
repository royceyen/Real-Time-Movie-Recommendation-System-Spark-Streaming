# Databricks notebook source
# MAGIC %md
# MAGIC ## JOB RUN 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics Evaluation

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, explode, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType

# Load data
ratings_df = spark.read.format("delta").load("/mnt/trends/delta/ratings")
movies_df = spark.read.csv("/mnt/trends/dataset/movie.csv", header=True, inferSchema=True)

# Split data
(train_df, test_df) = ratings_df.randomSplit([0.8, 0.2], seed=42)

# ALS model setup
als = ALS(
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True,
)

# Fit model
model = als.fit(train_df)

# Predict
predictions = model.transform(test_df)

# Evaluate
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print(f"✅ Best RMSE on test set: {rmse}")

# ---------------------------------------------
# Step 2: Save RMSE to Azure SQL DB
# ---------------------------------------------
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

# Create RMSE result DataFrame
rmse_data = [(rmse, )]
rmse_df = spark.createDataFrame(rmse_data, ["rmse"])
rmse_df = rmse_df.withColumn("evaluation_time", current_timestamp())

# Write to SQL table
rmse_df.write \
    .mode("append") \
    .jdbc(url=jdbc_url, table="als_evaluation", properties=connection_properties)


# COMMAND ----------

# MAGIC %md
# MAGIC ### RUNNING THE ALS MODEL THROUGH THE TRIGGERED JOB

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode, col

ratings_df = spark.read.format("delta").load("/mnt/trends/delta/ratings")
movies_df = spark.read.csv("/mnt/trends/dataset/movie.csv", header=True, inferSchema=True)

als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop", nonnegative=True)
model = als.fit(ratings_df)


user_recs = model.recommendForAllUsers(10)
flat_recs = user_recs.select(col("userId"), explode("recommendations").alias("rec")).select(
    "userId", col("rec.movieId").alias("movieId"), col("rec.rating").alias("score")
)
final_recs = flat_recs.join(movies_df, on="movieId")
final_recs.write.mode("overwrite").parquet("/mnt/blob/recommendations/user_recs.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### WRITING THE RECOMMENDATIONS TO SQL DATABASE

# COMMAND ----------

# Connection details
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

# # Example: write DataFrame to Azure SQL DB
# final_recs.write \
#     .mode("overwrite") \
#     .jdbc(url=jdbc_url, table="final_recommendations", properties=connection_properties)
    
from pyspark.sql.functions import explode, col, current_timestamp
final_recs_2 = flat_recs.join(movies_df, on="movieId").withColumn("prediction_timestamp", current_timestamp())

final_recs_2.write \
    .mode("append") \
    .jdbc(url=jdbc_url, table="final_recommendations_2", properties=connection_properties)


# COMMAND ----------

