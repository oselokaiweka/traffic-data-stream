# Streaming traffic data producer / simulator
import logging
from time import sleep, time
from datetime import timedelta, datetime
from random import random, randint, choice,  randrange
from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC='live-traffic-stream'

# Set up logging
logging.basicConfig(
    level=logging.INFO, # Level set to INFO, can be set to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s  -  %(message)s',
    filename='/home/oseloka/pyprojects/airflow/tolldata-etl/toll-app-log/live-traffic-producer.log',
    filemode='w'
)
# Create logger 
logger = logging.getLogger('live_traffic_producer_logger')
# Create a FileHandler with immediate flushing
file_handler = logging.FileHandler('live-traffic-producer.log', mode='w')
logger.addHandler(file_handler)


# define Kafka consumer configuration
def setup_kafka_producer():
    return KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9093']
    )

def start_kafka_producer():
    VEHICLE_TYPES = ("car", "car", "car", "car", "car", "car", "car", "car",
                    "car", "car", "car", "truck", "truck", "truck",
                    "truck", "van", "van")

    start_time = datetime.now()
    x = 1
    while datetime.now() < start_time + timedelta(hours=3):
        vehicle_id = randint(10000, 10000000)
        vehicle_type = choice(VEHICLE_TYPES)
        capture_point = randrange(1,99743,9)
        payload = []
        for i in range(0,15):
            record = {
                'vehicle_id':vehicle_id, 
                'vehicle_type':vehicle_type,  
                'capture_point':capture_point
            }
            payload.append(record)
            capture_point += 3
        try:
            producer = setup_kafka_producer()
            producer.send(TOPIC, value=str(payload).encode('utf-8'))
            logger.info(f" Total of {x*15} records has been sent")
            x += 1
            sleep(random() * 1.2)
        except KafkaError as ke:
            logger.error(f"Kafka Error: {ke}")
        except Exception as error:
            logger.error(f"Error sending payload: {error}")

if __name__=="__main__":
    start_kafka_producer()