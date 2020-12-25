#--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars mysql-connector-java-8.0.20.jar
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, element_at, split, array_min, array_max, array_remove
from pyspark.sql.types import MapType, StringType, ArrayType, FloatType
import pickle

def foreach_jdbc_writer(df,epoch_id):
    print(df.count())
    if (df.count() > 0):
        print('pushed data')
        df.write.\
        jdbc(url="jdbc:mysql://localhost/world",table="amazon_products",mode='append',properties={"driver":"com.mysql.cj.jdbc.Driver","user":"ahmed"})

spark = SparkSession.builder.master('local[2]').appName('StreamingDemo').getOrCreate()

df = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers','localhost:9092')\
    .option('subscribe','test')\
    .load()

deser = udf(lambda x: pickle.loads(x) ,MapType(StringType(),StringType()))

deserlizedDF = df.withColumn('map',deser(df['value']))
parsedDF = deserlizedDF.withColumn('title',element_at('map','productTitle'))\
    .withColumn('Categories',element_at('map','productCategories'))\
    .withColumn('Rating',element_at('map','productRating')).withColumn('Description',element_at('map','productDescription'))\
    .withColumn('Prices',element_at('map','productPrices'))\
    .withColumn('Min_Price',array_min(split(element_at('map','productPrices'),r'#*\$').cast(ArrayType(FloatType()))))\
    .withColumn('Max_Price',array_max(split(element_at('map','productPrices'),r'#*\$').cast(ArrayType(FloatType()))))

projectedDF = parsedDF.select('title','Categories','Rating','Prices','Min_Price','Max_Price')

result = projectedDF.writeStream.foreachBatch(foreach_jdbc_writer).start()   

result.awaitTermination()
