import os
import time
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col

KAFKA_BROKER = '172.29.112.212:9092'
DEST_KAFKA_BROKER = KAFKA_BROKER

SOURCE_TOPIC = 'first-topic'
DEST_TOPIC = 'second-topic'

def read_from_kafka(numPartitions: int) -> DataFrame:
    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("startingOffsets", "latest")
        .option("numPartitions", f"{numPartitions}")
        .option("checkpointLocation", "/home/user/kafka-trace-in/read/5")
        .option("failOnDataLoss", "false")
        .option("subscribe", SOURCE_TOPIC)
        .load()
        .select(
            col("value").cast("string").alias("body")
        )
    )
    return df

def process_each_batch(batch_df: DataFrame, batch_id) :
    batch_time = str(datetime.now()).split('.')[0]
    print(f"Time: {batch_time} Id: {batch_id}")

    (
        batch_df
        .selectExpr("to_json(struct(*)) as value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", DEST_KAFKA_BROKER)
        .option("topic", DEST_TOPIC)
        .option("batchsize", "50000")
        .mode("append")
        .save()
    )

if __name__ == '__main__':

    maxNumExecutors = 2
    # num max executors = 2
    # each 512m + 1 cpu
    # 1g + 2 core
    numPartitions = 2 * maxNumExecutors


    spark = (SparkSession
            .builder
            .appName(f"spark-monitoring-lab-job")
            .config("spark.driver.memory", "1g")
            .config("spark.driver.cores", "2")
            .config("spark.driver.memoryOverhead", "2g") # spark.driver.memoryOverhead = 2 * spark.driver.memory
            .config("spark.sql.shuffle.partitions", numPartitions)
            .config("spark.default.parallelism", numPartitions)
            .getOrCreate())

    i = 0
    while True:
        i += 1
        try:
            result = read_from_kafka(numPartitions)
            result.writeStream.foreachBatch(process_each_batch).trigger(processingTime="1 minute").start().awaitTermination()
        except Exception as e: 
            print("got an error which broke the pipeline!")
            print(f'error {i}: {e}')
            print("sleeping a 10 seconds before re-running the pipeline")
            time.sleep(10)
