import argparse
import configparser
import logging
import os
from logging.config import fileConfig
from os import path

from utilities.schema import get_schema, TIMESTAMP, URL, REF_URL, PROCESSED_TIMESTAMP, PROCESSED_URL, YOUTUBE_ID, \
    DOMAIN, PROCESSED_REF_URL, NULLVALUE, SEP
from utilities.utility import get_spark_session, transform_timestamp, transform_url, get_youtube_video, get_url_domain, \
    get_domain_list_by_request

log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging_config.ini')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger(__name__)

"""
    Application entry point
"""


def main():
    # Getting path as argument, path in this exercise is the S3 bucket where all the files are located
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Input file path")
    args = parser.parse_args()
    path = args.path

    logger.info("Starting application, PATH: %s" % path)

    # Reading AWS credentials, I am running the app. in my local PC. I created a test user in my AWS account
    # When running in EMR, this is not required as the cluster usually has the right ROLE for accessing S3 data
    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))
    aws_profile = 'default'

    access_id = config.get(aws_profile, "aws_access_key_id")
    access_key = config.get(aws_profile, "aws_secret_access_key")

    # Getting spark session
    spark = get_spark_session(access_id, access_key)

    # Reading the data from S3. Please note that here I am setting the separator and the null value as well as the
    # schema (see get_schema function)
    raw_data = spark.read.csv("s3a://%s/" % path, header=False, sep=SEP, schema=get_schema(), nullValue=NULLVALUE)

    # Processing phase

    # Timestamp requirement
    data = transform_timestamp(raw_data, TIMESTAMP, PROCESSED_TIMESTAMP)

    # Url requirements
    data = transform_url(data, PROCESSED_URL, URL)
    data = transform_url(data, PROCESSED_REF_URL, REF_URL)

    # Youtube Video and URL Domain
    data = get_youtube_video(data, YOUTUBE_ID, URL)
    data = get_url_domain(data, DOMAIN, REF_URL)

    # Just show a subset for debugging
    data.show()

    # Bonus point
    res = get_domain_list_by_request(data, DOMAIN)

    logger.info("Printing Domains by requests")
    for i in res:
        logger.info(str(i[0]) + " " + str(i[1]))

    # Could write to S3, but am just writing to local file system
    data.write.parquet("output/annalect.parquet")

    logger.info("Application has done")


if __name__ == '__main__':
    main()
