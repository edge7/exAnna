from collections import OrderedDict

from pyspark.sql.types import StructField, StringType, StructType, LongType
# Schema separator and null value

NULLVALUE = "\\N"
SEP = '\01'

# Schema Fields
REF_URL = 'REF_URL'
URL = 'URL'
TIMESPENT = 'Timespent'
TIMESTAMP = 'Timestamp'
UNKNOWN2 = 'UNKNOWN2'
UNKNOWN1 = 'UNKNOWN1'
BROWSER = 'Browser'
AGE = 'Age'
GENDER = 'Gender'
GUID = 'GUID'
PLATFORM = 'Platform'
CODE = 'CountryCode'

# Schema Fields to add
DOMAIN = "Domain"
YOUTUBE_ID = "YouTubeID"
PROCESSED_REF_URL = "PROCESSED_REF_URL"
PROCESSED_URL = "PROCESSED_URL"
PROCESSED_TIMESTAMP = "ProcessedTimestamp"

input_schema = OrderedDict([(('%s' % CODE), StringType()), (('%s' % PLATFORM), StringType()),
                            (('%s' % GUID), StringType()), (('%s' % GENDER), StringType()),
                            (('%s' % AGE), StringType()), (('%s' % BROWSER), StringType()),
                            (('%s' % UNKNOWN1), StringType()), (('%s' % UNKNOWN2), StringType()),
                            (('%s' % TIMESTAMP), LongType()), (('%s' % TIMESPENT), LongType()),
                            (('%s' % URL), StringType()), (('%s' % REF_URL), StringType())])


def get_schema():
    """

    :return: A Spark schema
    """
    fields = [StructField(field, type, True) for field, type in input_schema.items()]
    return StructType(fields)
