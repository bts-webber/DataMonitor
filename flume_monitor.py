#coding=utf-8
import requests,json
from requests.exceptions import ConnectionError
class flume_monitor(object):
    def __init__(self,hosts=[]):
        self.hosts=hosts
        self.metrics=self.getMetrics()
    def getMetrics(self):
        metrics={}
        for host in self.hosts:
            try:
                r=requests.get("http://"+host+"/metrics")
            except ConnectionError:
                metrics[host]="Conection Failed!"
            else:
                if r.status_code==200:
                    metrics[host]=json.loads(r.text)
                else:
                    metrics[host]="Page status code is :"+str(r.status_code)
        return metrics
    def monitor(self):
        result={}
        metrics=self.metrics
        for host in self.hosts:
            result[host]={}
            if isinstance(metrics[host],dict):
                try:
                    result[host]["Channel_put_speed"]=metrics[host]["SPEED"]["CHANNEL.ch2_EventPutSuccessCount_SPEED"]
                    result[host]["Channel_take_speed"]=metrics[host]["SPEED"]["CHANNEL.ch2_EventTakeSuccessCount_SPEED"]
                    metrics[host]["SPEED"].pop("CHANNEL.ch2_EventPutSuccessCount_SPEED")
                    metrics[host]["SPEED"].pop("CHANNEL.ch2_EventTakeSuccessCount_SPEED")
                except KeyError:
                    result[host]["Channel_put_speed"] = metrics[host]["SPEED"]["CHANNEL.ch2.EventPutSuccessCount.SPEED"]
                    result[host]["Channel_take_speed"] = metrics[host]["SPEED"]["CHANNEL.ch2.EventTakeSuccessCount.SPEED"]
                    metrics[host]["SPEED"].pop("CHANNEL.ch2.EventPutSuccessCount.SPEED")
                    metrics[host]["SPEED"].pop("CHANNEL.ch2.EventTakeSuccessCount.SPEED")
                    for i in metrics[host]["SPEED"].keys():
                        if "error" in i:
                            metrics[host]["SPEED"].pop(i)
                print metrics[host]["SPEED"]
                sink_speed=[]
                for i in metrics[host]["SPEED"].values():
                    if i[:-3]=="":
                        result[host]["Sink_sum_speed"] = u"0.0条/秒"
                    else:
                        sink_speed.append(float(i[:-3]))
                        result[host]["Sink_sum_speed"]=str(sum(sink_speed))+u"条/秒"
            else:
                result[host]=self.metrics[host]
        return result






