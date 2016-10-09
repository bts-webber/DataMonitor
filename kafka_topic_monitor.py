#coding=utf-8
import pykafka
from pykafka.exceptions import  NoBrokersAvailableError
class kafka_topic_monitor(object):
    def __init__(self,kafka_hosts):
        try:
            self.client=pykafka.KafkaClient(kafka_hosts)
        except NoBrokersAvailableError:
            self.offsets_sum="Kafka Connection Failed!"
        else:
            self.topics=self.client.topics.keys()
            self.offsets_sum=None
    def get_offsets_sum(self,topic_name):
        if self.offsets_sum=="Kafka Connection Failed!":
            offsets_sum=self.offsets_sum
        else:
            if topic_name not in self.topics:
                offsets_sum = "Not Found Topic!"
            else:
                topic=self.client.topics[topic_name.encode("utf-8")]
                offsets=topic.latest_available_offsets()
                for i in offsets.keys():
                    try:
                        offsets[i]=offsets[i][0][0]-1
                    except IndexError:
                        offsets[i]=0
                offsets_sum=sum(offsets.values())
        return offsets_sum