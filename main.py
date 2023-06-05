# importing packages
import gc
import os
import sys

from heapq import nlargest
from pyspark import SparkConf
from pyspark.sql import SparkSession, Column, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import concat_ws, when, col
from difflib import SequenceMatcher
from pyspark.context import SparkContext
from numpy import unique
from datetime import datetime, timedelta
import gc
from google.cloud import bigquery
import pandas as pd
from google.cloud.bigquery import dbapi

is_local = os.getenv('APP_ENV') == 'local'
current_dir = os.path.dirname(__file__)
matches_cache = {}


def get_close_matches(word, possibilities, n=3, cutoff=0.6):
    if not n > 0:
        raise ValueError("n must be > 0: %r" % (n,))
    if not 0.0 <= cutoff <= 1.0:
        raise ValueError("cutoff must be in [0.0, 1.0]: %r" % (cutoff,))
    result = []
    s = SequenceMatcher()
    s.set_seq2(word)
    for x in possibilities:
        s.set_seq1(x)
        if s.real_quick_ratio() >= cutoff and \
                s.quick_ratio() >= cutoff and \
                s.ratio() >= cutoff:
            ratio = s.ratio()
            result.append((ratio, x))
            if ratio == 1.0:
                break
    # Move the best scorers to head of list
    result = nlargest(n, result)
    # Strip scores for the best n matches
    return [x for score, x in result]


class ProductsCleaner:
    df = None

    def write_table_to_bigquery(self, mode, dataset, table, bucket):
        self.df.write. \
            format("bigquery"). \
            mode(mode). \
            option("checkpointLocation", "gs://{0}/{1}".format(bucket, "restore-point")). \
            option("temporaryGcsBucket", bucket). \
            save("{0}.{1}".format(dataset, table))

    def handle(self, spark, source_path, products_path, bucket, dataset, table, dates_path):
        dates = spark.read.csv(dates_path,
                               header=True, inferSchema=True)

        for row in dates.select('dates').collect():
            self.df = spark.read.format('bigquery') \
                .option("table", source_path) \
                .option("filter", f"date(sale_date) = '{row.dates}'") \
                .load()

            print("Processing data for", row.dates)

            product_names_df = spark.read.csv(products_path, header=True, inferSchema=True, sep=','). \
                select('product_name')

            product_names = [
                row.product_name.split(';')[0]
                for row in product_names_df.select('product_name').collect()
            ]
            manufacturer_names = [
                row.product_name.split(';')[1] if len(
                    row.product_name.split(';')) > 1 else ''
                for row in product_names_df.select('product_name').collect()
            ]
            manufacturer_names = unique(manufacturer_names).tolist()

            # defining udf to get product close matches
            def get_closest_match(word, possibilities: list[str]):
                word = str(word).lower()
                if found := matches_cache.get(word):
                    return found

                matches = get_close_matches(word, possibilities, n=1, cutoff=0.0)
                match = matches[0] if matches else ''
                score = round(SequenceMatcher(None, word, match).ratio(), 2)
                found = {'best_match': match, 'best_score': score}
                matches_cache[word] = found

                return found

            struct = StructType([
                StructField('best_match', StringType(), True),
                StructField('best_score', FloatType(), True),
            ])

            # defining udf to extract the product name from best_match
            product_name_udf = udf(
                lambda i: get_closest_match(i, product_names), struct)
            # defining udf to extract the manufacturer name from best_match
            manufacturer_name_udf = udf(
                lambda i: get_closest_match(i, manufacturer_names), struct)

            # Apply get_closest_match udf to product_manufacturer column
            self.df = self.df.withColumn('best_product_match_array', product_name_udf(col('product_name'))). \
                withColumn('best_manufacturer_match_array', manufacturer_name_udf(col('manufacturer_name'))). \
                withColumn('best_product_match', col('best_product_match_array')['best_match']). \
                withColumn('best_manufacturer_match', col('best_manufacturer_match_array')['best_match']). \
                withColumn('product_match_score', col('best_product_match_array')['best_score']). \
                withColumn('manufacturer_match_score', col('best_manufacturer_match_array')['best_score']). \
                drop('best_manufacturer_match_array'). \
                drop('best_product_match_array')

            self.write_table_to_bigquery(
                mode="append", dataset=dataset, table=table, bucket=bucket)

            self.df.unpersist()
            gc.collect()


if __name__ == '__main__':
    tmp_bucket = 'iprocure-edw'
    dataset_name = 'iprocure_edw'
    table_name = 'stockist_sales_transactions_cleanup_fulltable'

    conf = SparkConf(). \
        set("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar"). \
        set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"). \
        set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"). \
        set("spark.sql.execution.arrow.pyspark.enabled", "true")

    context = SparkContext(conf=conf)
    instance = SparkSession(context)

    if is_local:
        prod_names_path = f'file:///{current_dir}/data/final_product_list.csv'
        seed_data_path = f'file:///{current_dir}/data/feed_data.csv'
        transaction_dates_path = f'file:///{current_dir}/data/transaction_dates.csv'
    else:
        seed_data_path = f'gs://{tmp_bucket}/data-cleaning/feed_data.csv'
        prod_names_path = f'gs://{tmp_bucket}/data-cleaning/final_product_list.csv'
        transaction_dates_path = f'gs://{tmp_bucket}/data-cleaning/transaction_dates.csv'

    source_data_path = 'iprocure-edw.iprocure_wh_reporting.stockist_sale_transactions'

    ProductsCleaner().handle(instance, source_data_path, prod_names_path,
                             tmp_bucket, dataset_name, table_name, transaction_dates_path)
    instance.stop()