# 1. Dataset Example

## Instruction

1. Install pyspark
2. Download dataset from [Kagle](https://www.kaggle.com/datasets)
3. Create `SparkSession`
4. Read `csv` dataset
5. Print Schema and Show a bit of data
6. Write dataset in formats `json`, `parquet`, `orc`
7. Query Parquet file with the console `parquet-tools`.

## Spark

In this example we're using Spark for reading the example Dataset 
in `csv` format and writing it as an output in another file 
formats (`avro`, `parquet`, `orc`).

[Overview](https://spark.apache.org/docs/latest/index.html)

Install Spark

```shell
pip install pyspark
```

Import Spark components
```python
from pyspark.sql import DataFrame, SparkSession
```

Create local `SparkSession` - the driver component in Spark application.
```python
spark = SparkSession.builder.master("local").appName("App").getOrCreate()
```


Read the `csv` Dataset into Spark DataFrame.
```python
df:DataFrame = spark.read.option("header", True).csv("folder/file_location")
```

The `.option("header", True)` refers that first raw of the `csv` file is a header. 

Print Schemma
```python
df.printSchema()
```

Preview a piece of data
```python
df.show()
```

Write dataset in formats `json`
```python
df.write.json(location, "overwrite")
```

Close the SparkSession
```python
spark.stop()
```


## Parquet

Tool for queriing and reading `parquet` files
[parquet-tools](https://pypi.org/project/parquet-tools/)

Install tool:
```shell
pip install parquet-tools
```

Show parquet file **data**
```shell
 parquet-tools show file_name.parquet
```

Show parquet file **metadata**
```shell
 parquet-tools inspect file_name.parquet
```
