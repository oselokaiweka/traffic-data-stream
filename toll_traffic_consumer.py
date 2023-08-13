#Streaming traffiic data consumer / loader
import json
import logging
from datetime import datetime
from mysql.connector import Error
from mysql_pool import POOL
from kafka import KafkaConsumer
from kafka.errors import KafkaError

TOPIC='live-traffic-stream'

# Set up logging
logging.basicConfig(
    level=logging.INFO, # Level set to INFO, can be set to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s  -  %(message)s',
    filename='/home/oseloka/pyprojects/airflow/tolldata-etl/toll-app-log/live-traffic-consumer.log',
    filemode='w'
)
# Create logger 
logger = logging.getLogger('live_traffic_consumer_logger')
# Create a FileHandler with immediate flushing
file_handler = logging.FileHandler('live-traffic-consumer.log', mode='w')
logger.addHandler(file_handler)


# define Kafka consumer configuration
def setup_kafka_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=['localhost:9092', 'localhost:9093'],
        group_id='traffic_consumer_1',
        auto_offset_reset='earliest'
    )

def start_kafka_consumer():
    # Defining mysql insert statement function
    def insert_record(cursor, capture_time, vehicle_id, vehicle_type, capture_point):
        insert_statement =  """insert into live_traffic_stream
        (Capture_time, VehicleID, Vehicle_type, Capture_point)
        values (%s, %s, %s, %s);"""
        values = (capture_time, vehicle_id, vehicle_type, capture_point)
        cursor.execute(insert_statement, values)

    # Acquiring a connection from connection pool
    try:
        pool = POOL
        connection = pool.get_connection()
    except Exception as e:
        pool.add_connection()
        logger.info("Added a new connection")
        try:
            connection = pool.get_connection()
        except Error as pool_err:
            logger.critical(f"Pool conx error: {pool_err}")
            exit(1)

    logger.info(f"connected to {pool.pool_name}")
    cursor = connection.cursor()

    # Define function to get current time and format down to milli-second
    # this will be used to implement record expiiration trigger in database
    def get_timestamp():
        current_time = datetime.now()
        format_time = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] # 3 digit milli-sec
        return format_time

    try:
        # Connecting to kafka and starting the consumer
        consumer = setup_kafka_consumer()
        logger.info(f"Connected to Kafka topic: {TOPIC}")

        # Extracting data
        for msg in consumer:
            try:
                payload_list = json.loads(msg.value.decode('utf-8').replace("'", '"'))
                # Iterate through each dictionary in the payload list and insert each into database
                for record in payload_list:
                    capture_time = get_timestamp()
                    vehicle_id = record['vehicle_id']
                    vehicle_type = record['vehicle_type']
                    capture_point = record['capture_point']
                    insert_record(cursor, capture_time, vehicle_id, vehicle_type, capture_point)
                connection.commit()
                logger.info("Payload records inserted into database.")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
        
    except KafkaError as ke:
        logger.error(f"Kafka Error: {ke}")
    except Exception as error:
        logger.error(f"Error: {error}")
    finally:
        # Closing cursor and returning connection to pool
        cursor.close()
        connection.close()
        logger.info("Connection to database pool has been returned.")

if __name__=="__main__":
    start_kafka_consumer()