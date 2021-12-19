from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Aggregate0", label = "Aggregate0", x = 693, y = 193, phase = 0)
def Aggregate0(spark: SparkSession, _in: DataFrame) -> Aggregate:
    dfGroupBy = _in.groupBy(col("customer_id"))
    out = dfGroupBy.agg(
        max(col("last_name")).alias("last_name"),
        max(col("first_name")).alias("first_name"),
        sum(col("amount")).alias("amount")
    )

    return out
