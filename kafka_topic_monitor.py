#coding=utf-8
import pykafka
from pykafka.exceptions import  NoBrokersAvailableError
class kafka_topic_monitor(object):
    def __init__(self,kafka_hosts):
        self.kafka_hosts=kafka_hosts

    def get_offsets_sum(self,topic_name):
        try :
            client=pykafka.KafkaClient(self.kafka_hosts)
        except NoBrokersAvailableError:
            offsets_sum="Kafka Connection Failed!"
        else:
            if topic_name not in client.topics.keys():
                offsets_sum = "Not Found Topic!"
            else:
                topic=client.topics[topic_name.encode("utf-8")]
                offsets=topic.latest_available_offsets()
                for i in offsets.keys():
                    try:
                        offsets[i]=offsets[i][0][0]-1
                    except IndexError:
                        offsets[i]=0
                offsets_sum=sum(offsets.values())
        return offsets_sum


