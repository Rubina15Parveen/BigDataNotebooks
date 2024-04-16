from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from kafka import KafkaProducer
import spacy
import json

#Creating a Kafka producer to send data to brokers on localhost
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: str(v).encode('utf-8')
)

#Creating a spark session
spark = SparkSession.builder \
    .appName("KafkaNewsStreaming") \
    .getOrCreate()

BootstrapServers = "localhost:9092"
inputTopicKafka = "NewApiStreamInput"   #Kafka topic from which the input streaming data is taken

#Reads steaming data from Kafka into data frame streamingNews
streamingNews = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BootstrapServers) \
    .option("subscribe", inputTopicKafka) \
    .load()

#Transforms the news streams into string and renames the result column to news
transformedStreamDf = streamingNews \
    .selectExpr("CAST(value AS STRING) as news")

# NLP library for named entitiy recognition
ner = spacy.load("en_core_web_sm")


@udf(ArrayType(StringType()))
def wordExtraction(text):
    doc = ner(text)
    return [ent.text for ent in doc.ents]


# registers the udf to be used in spark SQL
spark.udf.register("wordExtraction", wordExtraction)

# Counting occurance of each word and ordering them in descending order of count
countWords = transformedStreamDf \
    .select("news", explode(wordExtraction("news")).alias("word")) \
    .groupBy("word") \
    .count() \
    .orderBy(col("count").desc())

# Displays the top 10 named entities
def displayWordCount(df, epoch_id):
    word_count = dict(df.take(10))
    producer.send('TopNewsOutPut', value=json.dumps(word_count).encode())
    print(word_count)


query = countWords.writeStream \
    .outputMode("complete") \
    .foreachBatch(displayWordCount) \
    .start()

query.awaitTermination()
