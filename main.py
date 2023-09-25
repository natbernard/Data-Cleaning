# importing packages
import os

from heapq import nlargest
from pyspark import SparkConf
from pyspark.sql import SparkSession, Column, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import concat_ws, when, col
from difflib import SequenceMatcher
from pyspark.context import SparkContext
from numpy import unique
from google.cloud import bigquery

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

    def handle(self, spark, source_path, products_path, bucket, dataset, table, structure, table_id):
        self.df = spark.read.format("bigquery")\
                       .option("table", table_id)\
                       .load()

        # self.df = spark.createDataFrame(rdd, schema=structure)

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

            matches = get_close_matches(word,
                                        possibilities,
                                        n=1,
                                        cutoff=0.0)
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
            withColumn('best_manufacturer_match_array', manufacturer_name_udf(col('product_manufacturer'))). \
            withColumn('best_product_match', col('best_product_match_array')['best_match']). \
            withColumn('best_manufacturer_match', col('best_manufacturer_match_array')['best_match']). \
            withColumn('product_match_score', col('best_product_match_array')['best_score']). \
            withColumn('manufacturer_match_score', col('best_manufacturer_match_array')['best_score']). \
            drop('best_manufacturer_match_array'). \
            drop('best_product_match_array')

        self.write_table_to_bigquery(mode="append",
                                     dataset=dataset,
                                     table=table,
                                     bucket=bucket)
        

if __name__ == '__main__':
    tmp_bucket = 'iprocure-edw'
    dataset_name = 'iprocure-edw.iprocure_edw'
    table_name = 'product_cleanup_1'
    source_table_id = 'iprocure-edw.iProcureMain.product'
    service_account = '/home/natasha/Documents/Iprocure/Sales-Data-Cleanup/bigquery_credentials/credentials.json'


    conf = SparkConf(). \
        set("spark.jars", "/home/natasha/envs/spark_env/gcs-connector-hadoop2-latest.jar," \
                        "/home/natasha/envs/spark_env/spark-3.3-bigquery-0.32.2.jar,"). \
        set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"). \
        set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"). \
        set("spark.sql.execution.arrow.pyspark.enabled", "true"). \
        set("spark.executor.memory", "8g"). \
        set("spark.driver.memory", "8g"). \
        set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", service_account)

    context = SparkContext(conf=conf)
    instance = SparkSession(context)

    prod_names_path = f'gs://{tmp_bucket}/data-cleaning/final_product_list.csv'
    source_data_path = f'gs://{tmp_bucket}/data-cleaning/subsequent_stockist_sale_transactions.parquet'

    schema = StructType([
        StructField('saletransaction_id', LongType(), True),
        StructField('product_id', LongType(), True),
        StructField('product_weight', DecimalType(), True),
        StructField('sub_category', StringType(), True),
        StructField('depot_name', StringType(), True),
        StructField('depot_id', LongType(), True),
        StructField('depot_type', LongType(), True),
        StructField('unit_cost', DecimalType(), True),
        StructField('unit_price', DecimalType(), True),
        StructField('vat', DecimalType(), True),
        StructField('quantity', LongType(), True),
        StructField('amount', DecimalType(), True),
        StructField('sale_type', LongType(), True),
        StructField('sale_date', TimestampType(), True),
        StructField('delete_status', LongType(), True),
        StructField('region_name', StringType(), True),
        StructField('region_id', LongType(), True),
        StructField('category_name', StringType(), True),
        StructField('product_type', StringType(), True),
        StructField('customer_name', StringType(), True),
        StructField('phone_number', StringType(), True),
        StructField('customer_auto_id', LongType(), True),
        StructField('gross_profit', DecimalType(), True),
        StructField('margin', DecimalType(), True),
        StructField('mpesa_code', StringType(), True),
        StructField('reference_code', StringType(), True),
        StructField('date_day', LongType(), True),
        StructField('date_month', LongType(), True),
        StructField('date_year', LongType(), True),
        StructField('sale_id', LongType(), True),
        StructField('sale_amount', DecimalType(), True),
        StructField('price_type', StringType(), True),
        StructField('promotion_status', LongType(), True),
        StructField('customer_type', LongType(), True),
        StructField('server_update_date', LongType(), True),
        StructField('invoice_id', LongType(), True),
        StructField('delivery_note_id', LongType(), True),
        StructField('product_type_number', StringType(), True),
        StructField('product_type_text', StringType(), True),
        StructField('unique_id', StringType(), True),
        StructField('country_name', StringType(), True),
        StructField('depot_county_name', StringType(), True),
        StructField('route_name', StringType(), True),
        StructField('town_name', StringType(), True),
        StructField('town_id', LongType(), True),
        StructField('route_id', LongType(), True),
        StructField('customer_creation_date', TimestampType(), True),
        StructField('agent_name', StringType(), True),
        StructField('agent_type', LongType(), True),
        StructField('agent_type_mode', LongType(), True),
        StructField('depot_registration_date', TimestampType(), True),
        StructField('product_name', StringType(), True),
        StructField('manufacturer_name', StringType(), True),
    ])

    ProductsCleaner().handle(instance, source_data_path, prod_names_path,
                             tmp_bucket, dataset_name, table_name, schema, source_table_id)
    instance.stop()