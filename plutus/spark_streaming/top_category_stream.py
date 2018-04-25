import argparse
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark import SparkContext


def decode_json(row):
    """
    :param row: A pyspark dataframe row
    """
    return row.decode("utf-8")


def main():
    sc = SparkContext()
    sc.setLogLevel("ERROR")

    spark = SparkSession.builder \
        .appName("top_category_stream") \
        .getOrCreate()
    parser = argparse.ArgumentParser()

    parser.add_argument("--topic", "-t", dest="topic",
                        help="kafka topic", required=True)
    cli_args = parser.parse_args()
    topic = cli_args.topic
    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .load()
    decode_json_udf = udf(decode_json)
    values_schema = StructType([
        StructField("device_category", StringType()),
        StructField("product_category", StringType()),
        StructField("amount", FloatType())])
    values = lines.select("value") \
        .withColumn("value", decode_json_udf("value")) \
        .select(from_json("value", values_schema).alias("parsed_value"))

    pc = values\
        .select("parsed_value.*")\
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    pc.awaitTermination()


if __name__ == "__main__":
    main()
