import time
from kafka import KafkaProducer
from newsapi import NewsApiClient

# Configure Kafka variables
BootstrapServers = "localhost:9092"  #You can write your bootstrap server here
inputTopicKafka = "NewApiStreamInput"     # You can replace this with your own input topic of kafka

# Configure NewAPI variables
newsApikey = "65fdde64cee04c6f9f3e27984769bd13"  # Replace with your API key
newsapi = NewsApiClient(api_key=newsApikey)


producer = KafkaProducer(
    bootstrap_servers=BootstrapServers,
    value_serializer=lambda v: str(v).encode('utf-8')
)

def newsHeadlinesStream():
    try:
        # Get the top headlines from country USA in the category Science in Engilish language
        topHeadlines = newsapi.get_top_headlines(country='us', category='general', language='en')

        # Streaming it to Kafka
        for headlines in topHeadlines['articles']:
            if (headlines['content']):
                producer.send(inputTopicKafka, value=headlines['content'])

        print("News Streaming on Kafka")

    except Exception as e:
        print(f"Error: {str(e)}")

# Schedule the job to run every 10 seconds
while True:
    newsHeadlinesStream()
    time.sleep(100)
