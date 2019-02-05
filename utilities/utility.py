import tldextract
from urllib.parse import urlparse, parse_qs, urljoin
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_unixtime
from pyspark.sql.types import StringType

# This is hardcoded for this exercise, as I am running in my laptop.
# When running in EMR that should be already included in the driver path
AWS_JAVA_SDK = "/home/edge7/Desktop/aws-java-sdk-1.7.4.jar"
HADOOP_AWS = "/home/edge7/Desktop/hadoop-aws-2.7.3.jar"


def get_spark_session(access, secret):
    """

    :param access: AWS access key
    :param secret: AWS secret Key
    :return: a Spark session with the right configuration for reading from S3 (Please note also that the Java JARs)
    """
    spark = SparkSession \
        .builder \
        .appName("Annalect Exercise") \
        .config('spark.driver.extraClassPath', "%s:%s" % (AWS_JAVA_SDK, HADOOP_AWS)) \
        .config('fs.s3a.access.key', access) \
        .config('fs.s3a.secret.key', secret) \
        .getOrCreate()

    return spark


def transform_timestamp(df, original, new_name):
    df = df.withColumn(new_name, from_unixtime(df[original] / 1000))
    return df


def transform_url(df, new_url, url):
    return df.withColumn(new_url, rm_query_string_udf(df[url]))


def get_url_domain(df, domain, url):
    return df.withColumn(domain, extract_domain(df[url]))


def get_domain_list_by_request(df, domain):
    return df.groupBy(domain).count().orderBy('count', ascending=False).collect()


def remove_querystring(x):
    if x is not None:
        return urljoin(x, urlparse(x).path)
    return x


def parse_youtube_video_id(x):
    if x is not None:
        if 'www.youtube.com/watch?v=' in x:
            url_data = urlparse(x)
            query = parse_qs(url_data.query)
            if 'v' in query:
                video = query["v"][0]
                return video
    return None


def get_youtube_video(df, yid, url):
    return df.withColumn(yid, get_youtube_video_id(df[url]))


def get_domain(x):
    if x is not None:
        res = tldextract.extract(x)
        if res.domain != "":
            return res.domain
        else:
            return None
    return x


rm_query_string_udf = udf(lambda z: remove_querystring(z), StringType())
get_youtube_video_id = udf(lambda x: parse_youtube_video_id(x), StringType())
extract_domain = udf(lambda x: get_domain(x), StringType())
