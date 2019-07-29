import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, when, lit, ceil, hypot, sum, max
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, BooleanType

from ncaa_basketball_schema import bb_schema

SCHEMA = StructType([
    StructField("player_id", StringType(), False),
    StructField("player_full_name", StringType(), False),
    StructField("shot_made", BooleanType(), False),
    StructField("three_point_shot", BooleanType(), False),
])

RIGHT_BASKET_X = 1065
LEFT_BASKET_X = 63
BASKET_Y = 50 * 12 / 2

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell'

if __name__ == '__main__':

    TOPIC_EVENTS = "ncaa_events"
    KAFKA_BROKERS = "localhost:9092"

    conf = SparkConf().setAppName("ncaa_basketball") \
        .setMaster("spark://asprague-laptop:7077")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("subscribe", TOPIC_EVENTS) \
            .load()

    events = stream.select(from_json(col("value").cast("string"), SCHEMA).alias("message")) \
        .select("message.player_id",
                "message.player_full_name",
                "message.shot_made",
                "message.three_point_shot",
                when(col("message.shot_made") == True, 1).otherwise(0).alias("fg_points"))
    
    shot_by_foot_percentrage = events \
        .withColumn("fg_value", when(col("three_point_shot") == True, 1.5).otherwise(1) * col("fg_points")) \
        .withColumn("attempt", lit(1)) \
        .groupBy("player_id") \
        .agg(max("player_full_name"), sum("fg_value"), sum("attempt")) \
        .withColumn("eFG%", col("sum(fg_value)") / col("sum(attempt)")) \
        .orderBy("sum(fg_value)", "eFG%", ascending=False) \
        .drop("player_id") \
        .withColumnRenamed("max(player_full_name)", "Player") \
        .withColumnRenamed("sum(fg_value)", "eFGM") \
        .withColumnRenamed("sum(attempt)", "FGA") \
        .writeStream \
        .format("console") \
        .outputMode("complete") \
        .trigger(processingTime="10 seconds") \
        .start() \
        .awaitTermination()