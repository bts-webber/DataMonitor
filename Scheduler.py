from flume_monitor import flume_monitor
from kafka_topic_monitor import kafka_topic_monitor
from elasticsearch_monitor import elasticsearch_monitor
import time
class Scheduler(object):
    def __init__(self, config):
        self.global_config = config["global"]
        self.flume_config = config["flume"]
        self.kafka_config = config["kafka"]
        self.es_config = config["elasticsearch"]
        self.date_now=time.strftime("%Y%m%d",time.localtime())
    def flume_scheduler(self):
        hosts = [self.flume_config["netflow"], self.flume_config["syslog"]]
        monitor = flume_monitor(hosts)
        return monitor.monitor()
    def kafka_scheduler(self):
        kafka_monitor=kafka_topic_monitor(self.global_config["kafka_hosts"])
        kafka_result={}
        for i in self.kafka_config:
            key=i.keys()[0]
            avro_offsets_sum=kafka_monitor.get_offsets_sum(i[key][0])
            enrich_offsets_sum=kafka_monitor.get_offsets_sum(i[key][1])
            kafka_result[key]=[avro_offsets_sum,enrich_offsets_sum]
        return kafka_result
    def es_scheduler(self):
        es_result={}
        es_monitor=elasticsearch_monitor(self.global_config["es_hosts"])
        for item in self.es_config:
            key=item.keys()[0]
            if key=="netflow":
                index_name="cnpc-netflow-bj-jingnei1-"+self.date_now
            else:
                index_name="cnpc-log-bj-jingnei1-"+self.date_now
            es_result[key]=es_monitor.get_doc_num(index_name,item[key])
        return es_result
    def scheduler(self):
        result = {}
        result["flume"] = self.flume_scheduler()
        result["kafka"] = self.kafka_scheduler()
        result["es"]=self.es_scheduler()
        return  result