from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("NBA Data to Hive") \
    .enableHiveSupport() \
    .getOrCreate()

grades_df = spark.read.option("header", True) \
    .csv("/usr/program/weeks_11_12/all_games.csv")

grades_df.write.mode("overwrite").saveAsTable("default.nbadata")

spark.sql("SELECT * FROM default.nbadata limit 10").show()

spark.stop()