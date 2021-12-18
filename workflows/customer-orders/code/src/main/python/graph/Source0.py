from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Source0", label = "Source0", x = 218, y = 128, phase = 0)
@UsesDataset(id = "7243", version = 0)
def Source0(spark: SparkSession) -> Source:
    fabric = Config.fabricName
    out = None

    if fabric == "Dev":
        schemaArg = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("account_open_date", StringType(), True),
            StructField("account_flags", StringType(), True)
        ])
        out = spark.read\
                  .format("csv")\
                  .schema(schemaArg)\
                  .option("header", True)\
                  .option("ignoreTrailingWhiteSpace", True)\
                  .option("sep", ",")\
                  .option("inferSchema", True)\
                  .option("ignoreLeadingWhiteSpace", True)\
                  .load("dbfs:/Prophecy/golam.rashed@outlook.com/CustomersDatasetInput.csv")
    else:
        raise ValueError("The fabric %s is not supported" % fabric)

    return out
