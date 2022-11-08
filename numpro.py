import random
import time
from kafka import KafkaProducer


bootstrap_server = ["localhost:9092"]
topic = "num"
producer = KafkaProducer(bootstrap_servers = bootstrap_server)
producer = KafkaProducer()

def senddata():
    values= random.randint(0,1000)
    
    message = producer.send(topic,bytes(str(values),"utf-8"))

    metadata = message.get()
    print(metadata.topic)
    print(metadata.partition)
    time.sleep(5)
while True:
    senddata()


