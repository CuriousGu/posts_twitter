from pyspark.sql import SparkSession, functions as f

spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()

lines = spark.readStream\
        .format("socket")\
        .option("host", "localhost")\
        .option('port', 9009)\
        .load()

words = lines.select(f.explode(f.split(lines.value, ' ')).alias('word'))
words_count = words.groupBy('word').count()

query = words_count.writeStream\
        .outputMode("complete")\
        .format("console")\
        .start()
    
query.awaitTermination()
