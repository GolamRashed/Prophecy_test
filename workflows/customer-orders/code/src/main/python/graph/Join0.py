from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Join0", label = "Join0", x = 512, y = 192, phase = 0)
def Join0(spark: SparkSession, left: DataFrame, right: DataFrame) -> Join:
    left_aliased = left.alias("left")
    right_aliased = right.alias("right")
    joined = left_aliased.join(right_aliased, (col("left.customer_id") == col("right.customer_id")), "inner")
    out = joined.select(
        col("left.customer_id").alias("customer_id"),
        col("left.last_name").alias("last_name"),
        col("right.amount").alias("amount"),
        col("left.first_name").alias("first_name")
    )

    return out
