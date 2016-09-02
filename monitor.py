#coding=utf-8
from Printer import Printer
import json
from Scheduler import Scheduler
from time import sleep
import time

def monitor(config):
    monitor_intervals=config["global"]["monitor_intervals"]
    last_result={}
    kafka_time_line={}
    es_time_line = {}
    start_time=time.strftime("%m-%d %X",time.localtime())

    while True:
        print 120 * "*"
        print 55*" " + "Started Monitor at %s" % start_time
        print 120 * "*"+3*"\n"
        scheduler=Scheduler(config)
        result=scheduler.scheduler()
        printer=Printer(config)
        printer.flume_printer(result)
        kafka_time_line=printer.kafka_printer(last_result,result,kafka_time_line)
        es_time_line=printer.es_printer(last_result,result,es_time_line)
        last_result=result
        print 4*"\n"
        sleep(monitor_intervals)

if __name__=="__main__":
    with open("config.json") as configFile:
        config=json.loads(configFile.read())
    monitor(config)



