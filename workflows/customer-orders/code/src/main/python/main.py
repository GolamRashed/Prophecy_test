from pyspark.sql import SparkSession


class Main:

    def graph(self, spark: SparkSession):
        df_Source1: Source = Source1(spark)
        df_Source0: Source = Source0(spark)

    def main(self):
        spark = SparkSession\
                    .builder()\
                    .appName("customerorders")\
                    .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                    .enableHiveSupport()\
                    .getOrCreate()
        spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
        self.graph(spark)

if __name__ == __main__:
    Main().main()
