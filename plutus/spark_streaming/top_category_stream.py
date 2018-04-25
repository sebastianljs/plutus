import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkContext


def decode_json(line):
    return line.decode("utf-8")


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
    product_categories = lines.select("value").withColumn("value", decode_json_udf("value"))
    pc = product_categories \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    pc.awaitTermination()


if __name__ == "__main__":
    main()
