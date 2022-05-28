from pyspark.sql import DataFrame, SparkSession

APP_NAME = "ReadWriteExample"
CSV_INP_LOCATION = "data/csv/all_players_stats.csv"
PARQUET_OUT_LOCATION = "data/parquet"
ORC_OUT_LOCATION = "data/orc"
AVRO_OUT_LOCATION = "data/avro"

spark: SparkSession = None


def create_spark(app_name: str) -> SparkSession:
    print("Creating Spark")
    return SparkSession.builder \
        .master("local") \
        .appName(app_name) \
        .getOrCreate()


def read_csv(location: str) -> DataFrame:
    print(f"Read csv Dataset by location: '{location}'")
    df = spark.read \
        .option("header", True) \
        .csv(location)
    return df


def info(df: DataFrame):
    print("\nSchema: ")
    df.printSchema()
    print("\nData preview: ")
    df.show()


def write_parquet(df: DataFrame, location: str):
    print(f"Write Dataset in Parquet format to location: '{location}'")
    df.write.parquet(location, "overwrite")


def write_orc(df: DataFrame, location: str):
    print(f"Write Dataset in ORC format to location: '{location}'")
    df.write.orc(location, "overwrite")


def write_avro(df: DataFrame, location: str):
    print(f"Write Dataset in Avro format to location: '{location}'")
    df.write.format("avro").save(location)


def stop():
    print("Stopping Spark")
    spark.stop()


def run():
    global spark
    spark = create_spark(APP_NAME)
    df = read_csv(CSV_INP_LOCATION)
    info(df)
    write_parquet(df, PARQUET_OUT_LOCATION)
    write_orc(df, ORC_OUT_LOCATION)
    # write_avro(df, AVRO_OUT_LOCATION)
    stop()
