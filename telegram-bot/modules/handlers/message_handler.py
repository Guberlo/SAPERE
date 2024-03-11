import hashlib
import json
import socket

from telegram import Update
from telegram.ext import CallbackContext

from elasticsearch import Elasticsearch

from confluent_kafka import Producer

from modules.data.config import Config

config = Config()
kafka_conf = {'bootstrap.servers': f"{config.kafka_host}:{config.kafka_port}",'client.id': socket.gethostname()}
producer = Producer(kafka_conf)
es_client = Elasticsearch(f"http://{config.es_host}:{config.es_port}") 


es_mapping = """{
                    \"properties\": {
                      \"document_id\": {
                        \"type\": \"keyword\"
                      },
                      \"text\": {
                        \"type\": \"keyword\"
                      },
                      \"join_field\": {
                        \"type\": \"join\",
                        \"relations\": {
                          \"document\": [
                            \"alg1\",
                            \"alg2\",
                            \"alg3\",
                            \"gero\"
                          ]
                        }
                      },
                      \"alg1\": {
                        \"type\": \"keyword\"
                      },
                      \"alg2\": {
                        \"type\": \"keyword\"
                      },
                      \"alg3\": {
                        \"type\": \"keyword\"
                      },
                      \"gero\": {
                        \"type\": \"nested\",
                        \"properties\": {
                          \"start\": {
                            \"type\": \"integer\"
                          },
                          \"end\": {
                            \"type\": \"integer\"
                          },
                          \"mention\": {
                            \"type\": \"keyword\"
                          },
                          \"entity_id\": {
                            \"type\": \"integer\"
                          },
                          \"entity_name\": {
                            \"type\": \"keyword\"
                          },
                          \"entity_type\": {
                            \"type\": \"keyword\"
                          },
                          \"rho\": {
                            \"type\": \"double\"
                          },
                          \"confidence\": {
                            \"type\": \"double\"
                          }
                        }
                      }
                    }
                  }
                }"""

async def elaborate_message(update: Update, context: CallbackContext):
    """Tries to save the message to Elasticsearch (if it does not exist yet)
    Then it tries to save the message into the 'requests' kafka topic
    Args:
        update: update event
        context: context passed by the handler
    """
    print(config.kafka_topic)
    message_id = hashlib.sha256(update.message.text.encode('utf-8')).hexdigest()
    if es_client.exists(index="requests", id=message_id):
        return
    else:
        es_client.index(index=config.es_index, id=message_id, body={'text': update.message.text})
        kafka_payload = {"id": message_id,"text": update.message.text}
        producer.produce(config.kafka_topic, value=json.dumps(kafka_payload))
