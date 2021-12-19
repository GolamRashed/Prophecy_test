from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Source1", label = "Source1", x = 222, y = 264, phase = 0)
@UsesDataset(id = "7244", version = 0)
def Source1(spark: SparkSession) -> Source:
    fabric = Config.fabricName
    out = None

    if fabric == "Dev":
        schemaArg = StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_category", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
        out = spark.read\
                  .format("csv")\
                  .schema(schemaArg)\
                  .option("header", True)\
                  .option("ignoreTrailingWhiteSpace", True)\
                  .option("sep", ",")\
                  .option("inferSchema", True)\
                  .option("ignoreLeadingWhiteSpace", True)\
                  .load("dbfs:/Prophecy/golam.rashed@outlook.com/OrdersDatasetInput.csv")
    else:
        raise ValueError("The fabric %s is not supported" % fabric)

    return out
