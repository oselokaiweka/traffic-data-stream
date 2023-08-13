import sys
import json
import pytest
import logging
from io import StringIO
from unittest.mock import MagicMock, patch, Mock
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError


# Import each of the python scripts to be tested
from toll_traffic_topic import create_topic
"""
from toll_traffic_producer import start_kafka_producer
from toll_traffic_consumer import start_kafka_consumer
from toll_traffic_consumer_deleter import start_kafka_data_remover
from mysql_pool import start_mysql_service, pool_connection
"""


#----------"toll_traffic_topic.py" test begins here----------

@pytest.fixture # Fixture to mock KafkaAdminClient
def mock_KafkaAdminClient(monkeypatch):
    class MockKafkaAdminClient():
        def __init__(self, *args, **kwargs):
            pass
        def create_topic(self, new_topics, validate_only=False, *args): # Mock create_topics method
            # Create a mock topic object for each new topic
            mock_topics = [
                NewTopic(
                name=topic.name, 
                num_partitions=topic.num_partitions, 
                replication_factor=topic.replication_factor
                ) 
            for topic in new_topics
            ]
            # Return a mock object that simulates the topic creation
            return Mock(create_topics_result=mock_topics)
    # Monkeypatch KafkaAdminClient to use Mock_KafkaAdminClient
    monkeypatch.setattr("kafka.admin.KafkaAdminClient", MockKafkaAdminClient)

# Test create_topic function if new topic is created successfully
def test_create_topic_success(caplog, mock_KafkaAdminClient):
    create_topic('test-topic-3', 2, 2, mock_KafkaAdminClient) # Action: Call create_topic function
    print(caplog.text)
    assert "Topic 'test-topic-3' created successfully!" in caplog.text # Assert: Check output


"""
# Test create_topic function on how it handles when a topic already exists
def test_topic_already_exists(caplog, mock_KafkaAdminClient):
    create_topic('existing-topic-3', 2, 2, mock_KafkaAdminClient) # Action: Call the create_topic function
    print(caplog.text)
    assert "Topic 'existing-topic-3' already exists" in caplog.text # Assert: Check output

# Test create_topic for handling KafkaError
def test_kafka_error(caplog, mock_KafkaAdminClient):
    class MockKafkaError(KafkaError):
        def __init__(self, *args, **kwargs):
            pass
        def __str__(self):
            return "Some Kafka Error"
    # Monkeypatch KafkaAdminClient.create_topics to use MockKafkaError
    mock_KafkaAdminClient.create_topic.side_effect = MockKafkaError()
    create_topic('error-topic', 2, 2, mock_KafkaAdminClient) # Action: Call create_topic function
    print(caplog.text)
    assert "Some Kafka Error" in caplog.text # Assert: Check output
    
#----------"toll_traffic_topic.py" test ends here----------
"""