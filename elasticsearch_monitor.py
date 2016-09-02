from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError,NotFoundError
class elasticsearch_monitor(object):
    def __init__(self,es_hosts):
        self.es_connect=Elasticsearch(es_hosts)
    def get_doc_num(self,index_name,type_name):
        try:
            doc_num=self.es_connect.count(index_name,type_name)

        except ConnectionError:
            return "Connection Error!"
        except NotFoundError:
            return "Not Found Index or Type!"
        else:
            return doc_num["count"]




