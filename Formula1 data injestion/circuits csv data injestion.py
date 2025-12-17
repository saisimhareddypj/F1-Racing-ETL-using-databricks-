# Databricks notebook source
# MAGIC %md
# MAGIC ### circuits data injestion

# COMMAND ----------

# MAGIC %md
# MAGIC ## steps for reading data
# MAGIC - first we create a data frame, here we mention the file path, if file has a header and then the schema of the file
# MAGIC - while mentioning schema we can use infer schema but it takes more time so it is recommended to create and use our own schema
# MAGIC - we can add new columns, rename the columns and make transformations needed as per our requirement before we finilize our data frame
# MAGIC - before creating the schema you can view the original schema of the data using print schema
# MAGIC - once your schema is ready and all the transformations are completed you will need to write your dataframe into a container where you want
# MAGIC

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1projectdata/raw

# COMMAND ----------

# MAGIC %run "../Formula1 configuration/includes"

# COMMAND ----------

# MAGIC %run "../Formula1 configuration/common functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
data_source = dbutils.widgets.get("p_data_source")
#adding a new column to our dataframe

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

#when we use infer schema, data bricks automatically assigns a schema, but this takes extra run time and is not recommended on large data sets

#circuits_df = spark.read.csv("/mnt/f1projectdata/raw/circuits.csv", header=True, inferSchema=True)
#display(circuits_df)

# COMMAND ----------

#struct type is like a table schema and it contains struct fields and struct fields are our columns, using this we build our schema

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

#defining our own schema

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                   StructField("circuitRef", StringType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("location", StringType(), True),
                                   StructField("country", StringType(), True),
                                   StructField("lat", DoubleType(), True),
                                   StructField("lng", DoubleType(), True),
                                   StructField("alt", IntegerType(), True),
                                   StructField("url", StringType(), True)])
                                  

# COMMAND ----------

#reading data
circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header=True, schema=circuits_schema)

# COMMAND ----------

#selecting columns from our dataframe
#when we use this method we are strictly restricted to just selecting our columns, we cannot do any transformations on our columns

#circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")


# COMMAND ----------

#when we use the col function we can do any transformations on our columns, for this we have to first import the col function

from pyspark.sql.functions import col, lit

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

#we use withColumnRenamed to rename our columns

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

#adding a new column to our dataframe

# COMMAND ----------

#reading data
#circuits_df = spark.read.csv(f"{raw_folder_path}/circuits.csv", header=True, schema=circuits_schema)

# COMMAND ----------

#withColumn is used to add a new column to the dataframe
circuits_added_df = add_injestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df = circuits_added_df.withColumn("datasource", lit(data_source))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import lit

#adding a new column to our dataframe, this time with a literal value
#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()
#                                                    .withColumn("env", lit("Production")))
#display(circuits_final_df)

# COMMAND ----------

#writing data to processed container in parquet format

#circuits_final_df.write.parquet(f"{processed_folder_path}/circuits", mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.circuits

# COMMAND ----------

#overwrite_partition(circuits_final_df, 'f1_processed', 'circuits', 'circuit_id')

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended f1_processed.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits
# MAGIC

# COMMAND ----------

# %fs
# ls /mnt/f1projectdata/processed/circuits

# COMMAND ----------

#df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

#display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")