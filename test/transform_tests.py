import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from utilities.utility import transform_timestamp, transform_url, get_youtube_video, get_url_domain


@pytest.fixture()
def spark():
    return SparkSession \
        .builder \
        .appName("test") \
        .getOrCreate()


def test_timestamp(spark):
    schema = StructType([StructField("TimeLong", LongType())])
    test_list = [1549378626000], [1549378627000]
    df = spark.createDataFrame(test_list, schema=schema)
    res = transform_timestamp(df, "TimeLong", "TimeNew")
    coll = res.collect()

    assert coll[0][1] == '2019-02-05 14:57:06'
    assert coll[0][0] == 1549378626000
    assert coll[1][0] == 1549378627000
    assert coll[1][1] == '2019-02-05 14:57:07'


def test_url_strip(spark):
    schema = StructType([StructField("URL", StringType())])

    test_list = [["https://m.facebook.com/pg/thaddaeuskoroma.page/videos/?ref=page_internal&mt_nav=1"],
                 [
                     "http://www.royalgames.com/spiele/wortspiele/letter-star/?action=play&templateId=11272&accepted=true"],
                 ["http://example.com/data.csv#row=4"]]

    df = spark.createDataFrame(test_list, schema=schema)
    data = transform_url(df, "NEW_URL", "URL")
    coll = data.collect()
    assert coll[0][0] == test_list[0][0]
    assert coll[0][1] == 'https://m.facebook.com/pg/thaddaeuskoroma.page/videos/'
    assert coll[1][0] == test_list[1][0]
    assert coll[1][1] == 'http://www.royalgames.com/spiele/wortspiele/letter-star/'
    assert coll[2][0] == test_list[2][0]
    assert coll[2][1] == 'http://example.com/data.csv'


def test_youtube(spark):
    schema = StructType([StructField("URL", StringType())])

    test_list = [["https://www.youtube.com/watch?v=y2vTdJgzioM"],
                 ["https://www.youtube.com/watch?v=pyXhsTTD0Tk&list=RDpyXhsTTD0Tk&index=33"]
                 ]
    df = spark.createDataFrame(test_list, schema=schema)
    data = get_youtube_video(df, 'VIDEO_ID', 'URL')
    coll = data.collect()
    assert coll[0][0] == test_list[0][0]
    assert coll[0][1] == 'y2vTdJgzioM'
    assert coll[1][0] == test_list[1][0]
    assert coll[1][1] == 'pyXhsTTD0Tk'


def test_domain(spark):
    schema = StructType([StructField("URL", StringType())])

    test_list = [["https://www.youtube.com/watch?v=y2vTdJgzioM"],
                 ["https://www.ed7London.co.uk/id?i=1"]
                 ]
    df = spark.createDataFrame(test_list, schema=schema)
    data = get_url_domain(df, 'DOMAIN', 'URL')
    coll = data.collect()
    assert coll[0][0] == test_list[0][0]
    assert coll[0][1] == 'youtube'
    assert coll[1][0] == test_list[1][0]
    assert coll[1][1] == 'ed7London'
