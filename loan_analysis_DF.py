#1. month in which maximum loan request submited in last one year (01-04-2019 to 31-03-2020)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import month
schema = StructType([
    StructField("application_id",StringType(),True),\
    StructField("customer_id",StringType(),True), \
    StructField("car_price",IntegerType(),True),\
  StructField("car_model",StringType(),True), \
  StructField("customer_location", StringType(),True),\
  StructField("request_date", DateType(),True),\
  StructField("loan_status", StringType(),True)
])

df = spark.read.csv("/FileStore/tables/auto_loan.csv", \
                   schema=schema,
                    header=True)
df1 = df.filter("request_date >= '2019-04-01' and request_date <= '2020-03-31'")
resDF = df1.select( df1.customer_id, df1.request_date).groupBy(month(df1.request_date)).count()
resDF.show()
finalDF = resDF.toDF("month","counter")
DF = finalDF.agg({'counter':"max"})
DF.show()
