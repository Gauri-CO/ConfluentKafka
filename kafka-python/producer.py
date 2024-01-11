from confluent_kafka import Producer
from config import config

# callback function
def callback(err,event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf8')
        print(f'{val} sent to partition {event.partition()}.')

#produce to topic
def say_hello(producer,key):
    value = f'Hello {key}!'
    producer.produce('hello_topic',value,key,on_delivery=callback)

if __name__=='__main__':
    producer = Producer(config)
    keys = ['Amy', 'Brenda', 'Cindy', 'Derrick', 'Elaine', 'Fred']
    for key in keys:
        say_hello(producer,key)
        producer.flush()








