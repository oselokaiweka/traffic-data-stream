import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# Set up logging
logging.basicConfig(
    level=logging.INFO, # Level set to INFO, can be set to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s  -  %(message)s',
    filename='/home/oseloka/pyprojects/airflow/tolldata-etl/toll-app-log/live-traffic-topic.log',
    filemode='w'
)
# Create logger 
logger = logging.getLogger('live_traffic_topic_logger')
# Create a FileHandler with immediate flushing
file_handler = logging.FileHandler('live-traffic-topic.log', mode='w')
logger.addHandler(file_handler)


def create_topic(t_name, num_par, rep_fac, *args):
    try:
        bootstrap_servers = ['localhost:9092', 'localhost:9093']
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topic_list =[]
        new_topic = NewTopic(name=t_name, num_partitions=num_par, replication_factor=rep_fac)
        topic_list.append(new_topic)
        admin_client.create_topics(topic_list)
        logger.info(f"Topic '{t_name}' created successfully!")
    except  TopicAlreadyExistsError:
        logger.error(f"Topic '{t_name}' already exists")
    except KafkaError as e:
        logger.error(e)
    except Exception as e:
        logger.critical(f"An error occured while creating topic '{t_name}': {str(e)}")
        exit(1)

if __name__=="__main__":
    create_topic('live-traffic-stream', 2, 2)