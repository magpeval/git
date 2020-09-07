import datetime
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, DoubleType
import argparse
import cfn

#----------------------------------
#Constructor de  argumentos parser 
#-----------------------------------
ap = argparse.ArgumentParser()
ap.add_argument("-a", "--file", required=True,
   help="first operand")
args = vars(ap.parse_args())


#-----------------------------------
# configuración conexión mongo
#------------------------------------
mongo_conn =cfn.MONGO_CONN
mongo_connector = 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.2'
mongo_db = "conekta"
mongo_coll = "stagin"

#-------------------------------------
# configuraciones spark 
#----------------------------------
spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", mongo_conn)\
    .config("spark.mongodb.output.uri", mongo_conn)\
    .config('spark.jars.packages',mongo_connector)\
    .getOrCreate()

sql = SQLContext(spark)

#----------------------------------
#creación de esquema para mongo
#----------------------------------
schema = StructType() \
      .add("id",StringType(),True) \
      .add("name",StringType(),True) \
      .add("company_id",StringType(),True) \
      .add("amount",DoubleType(),True) \
      .add("status",StringType(),True) \
      .add("created_at",StringType(),True) \
      .add("paid_at",StringType(),True) 
#--------------------------------------------
#   lectura de archivo data source  
#--------------------------------------------
df = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load(args['file'])

#-----------------------------------
# escritura de datos en mongo 
#-----------------------------------

df.write.format('com.mongodb.spark.sql.DefaultSource').mode('append')\
        .option('database',mongo_db).option('collection', mongo_coll).save()


#---------------------------------------------------------
# proceso de extracción y escritura a archivo parquet
#-----------------------------------------------------------
df_read = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df_read.write.parquet('./output/data')
parqDF = spark.read.parquet("./output/data")
parqDF.createOrReplaceTempView("ParquetData")


#--------------------------------------------------------
# proceso de limpieza y transformación de campos
#--------------------------------------------------------

df_prquet = spark.sql("select * from ParquetData ")
df_prquet =  df_prquet.withColumnRenamed("name","company_name")
df_prquet =  df_prquet.withColumn('created_at',df_prquet['created_at'].cast("Date"))
df_prquet =  df_prquet.withColumn('paid_at', df_prquet['paid_at'].cast("Date")).withColumnRenamed("paid_at", "updated_at")
df_prquet.printSchema()
df_prquet =  df_prquet.drop(df_prquet['_id'])


