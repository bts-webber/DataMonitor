#coding=utf-8
from prettytable import PrettyTable,MSWORD_FRIENDLY,PLAIN_COLUMNS
import time
class Printer(object):
    def __init__(self, config):
        self.global_config = config["global"]
        self.flume_config = config["flume"]
        self.kafka_config = config["kafka"]
        self.es_config = config["elasticsearch"]
    def red(self,s):
        return "\033[1;31m"+s+"\033[0m"
    def green(self,s):
        return "\033[1;32m"+s+"\033[0m"
    def yellow(self,s):
        return "\033[1;33m" + s + "\033[0m"
    def es_printer(self,last_result,result,time_line):
        now_time = time.strftime("%m-%d %X", time.localtime())
        print 40 * "=" + "   Elasticsearch   " + 40 * "="
        print 38 * "=" + "   ( %s )   " % now_time + 38 * "="
        table = PrettyTable()
        table.set_style(PLAIN_COLUMNS)
        table._set_field_names(["Type","Status","Last_doc_count","Now_doc_count","Last_doc_count_time"])
        if not last_result:
            result=result["es"]
            Last_doc_count="null"
            for key in result.keys():
                if isinstance(result[key],str):
                    Status=self.red("Error")
                    time_line[key] =""
                else:
                    Status=self.yellow("Unknow")
                    time_line[key] = now_time
                Now_doc_count = result[key]
                Last_doc_count_time = ""
                table.add_row([key,Status,Last_doc_count,Now_doc_count,Last_doc_count_time])

        else:
            result = result["es"]
            last_result=last_result["es"]
            for key in result.keys():
                if isinstance(result[key],str):
                    Status=self.red("Error")
                    Last_doc_count=last_result[key]
                    Now_doc_count=result[key]
                    Last_doc_count_time=""
                else:
                    if result[key]>last_result[key]:
                        Status=self.green("OK")
                        Last_doc_count_time =time_line[key]
                        time_line[key]=now_time
                    elif result[key]==0:
                        Status=self.red("Error")
                        Last_doc_count_time=time_line[key]
                        time_line[key]=now_time
                    else:
                        Status=self.yellow("Unknow")
                        Last_doc_count_time = time_line[key]
                    Last_doc_count=last_result[key]
                    Now_doc_count=result[key]
                table.add_row([key,Status,Last_doc_count,Now_doc_count,Last_doc_count_time])
        print table.get_string(sortby="Status")
        return time_line

    def kafka_printer(self,last_result,result,time_line):
        now_time=time.strftime("%m-%d %X",time.localtime())
        print 80 * "=" + "   KAFKA_TOPIC   " + 80 * "="
        print 76 * "=" + "   ( %s )   " %now_time + 76 * "="
        table = PrettyTable()
        table.set_style(PLAIN_COLUMNS)
        table._set_field_names(["Type","Avro_status","Avro_offsets_sum","Avro_Last_offsets_time",
                       "Enrich_status","Enrich_offsets_sum","Enrich_Last_offsets_time"])
        if not last_result:
            result = result["kafka"]
            Avro_last_offsets_sum="null"
            Enrich_last_offsets_sum="null"
            for key in result.keys():
                if isinstance(result[key][0],str):
                    Avro_status = self.red("Error")
                    Avro_now_offsets_sum = result[key][0]
                else:
                    Avro_status = self.yellow("Unknown")
                    Avro_now_offsets_sum = result[key][0]
                if isinstance(result[key][1], str):
                    Enrich_status = self.red("Error")
                    Enrich_now_offsets_sum = result[key][1]
                else:
                    Enrich_status = self.yellow("Unknown")
                    Enrich_now_offsets_sum = result[key][1]
                Avro_offsets_sum="Now:"+str(Avro_now_offsets_sum)
                Enrich_offsets_sum="Now:"+str(Enrich_now_offsets_sum)
                table.add_row([key,Avro_status,Avro_offsets_sum,"",
                               Enrich_status,Enrich_offsets_sum,""])
                time_line[key]={}
                time_line[key]["avro"]=now_time
                time_line[key]["enrich"]=now_time

        else:
            last_result=last_result["kafka"]
            result = result["kafka"]
            for key in result.keys():
                Avro_last_offsets_sum=last_result[key][0]
                Avro_now_offsets_sum=result[key][0]
                Enrich_last_offsets_sum=last_result[key][1]
                Enrich_now_offsets_sum=result[key][1]
                if isinstance(Avro_now_offsets_sum,str):
                    Avro_status = self.red("Error")
                    Avro_Last_offsets_time=""
                else:
                    if Avro_now_offsets_sum>Avro_last_offsets_sum:
                        Avro_status=self.green("OK")
                        time_line[key]["avro"]=now_time
                        Avro_Last_offsets_time = ""
                    elif Avro_now_offsets_sum<=0:
                        Avro_status=self.red("Error")
                        Avro_Last_offsets_time=""
                    else:
                        Avro_status=self.yellow("Unknow")
                        Avro_Last_offsets_time = self.yellow(time_line[key]["avro"])
                if isinstance(Enrich_now_offsets_sum, str):
                    Enrich_status = self.red("Error")
                    Enrich_Last_offsets_time=""
                else:
                    if Enrich_now_offsets_sum > Enrich_last_offsets_sum:
                        Enrich_status = self.green("OK")
                        time_line[key]["enrich"] = now_time
                        Enrich_Last_offsets_time = ""
                    elif Enrich_now_offsets_sum <= 0:
                        Enrich_status = self.red("Error")
                        Enrich_Last_offsets_time = ""
                    else:
                        Enrich_status = self.yellow("Unknow")
                        Enrich_Last_offsets_time = self.yellow(time_line[key]["enrich"])
                Avro_offsets_sum="Last:"+str(Avro_last_offsets_sum)+"\n"+\
                                 "Now:"+str(Avro_now_offsets_sum)
                Enrich_offsets_sum="Last:"+str(Enrich_last_offsets_sum)+"\n"+\
                                   "Now:"+str(Enrich_now_offsets_sum)
                table.add_row([key,Avro_status,Avro_offsets_sum,Avro_Last_offsets_time,
                               Enrich_status,Enrich_offsets_sum,Enrich_Last_offsets_time])
        print table.get_string(sortby="Avro_status")
        return time_line



    def flume_printer(self,result):
        print 65*"="+"   FLUME   "+65*"="
        table=PrettyTable()
        table.set_style(PLAIN_COLUMNS)
        table._set_field_names(["Type","Host","Status","Channel_put_speed","Channel_take_speed","Sink_sum_speed"])
        for i in self.flume_config.keys():
            if isinstance(result["flume"][self.flume_config[i]],dict):
                if (result["flume"][self.flume_config[i]]["Channel_put_speed"]!=u"0.0条/秒" \
                        and result["flume"][self.flume_config[i]]["Channel_take_speed"]!= u"0.0条/秒" \
                        and result["flume"][self.flume_config[i]]["Sink_sum_speed"]!= u"0.0条/秒"):
                    status=self.green("OK")
                else:
                    status=self.red("ERROR")
                Channel_put_speed=result["flume"][self.flume_config[i]]["Channel_put_speed"]
                Channel_take_speed=result["flume"][self.flume_config[i]]["Channel_take_speed"]
                Sink_sum_speed=result["flume"][self.flume_config[i]]["Sink_sum_speed"]
            else:
                status=self.red(result["flume"][self.flume_config[i]])
                Channel_put_speed = "null"
                Channel_take_speed = "null"
                Sink_sum_speed = "null"
            table.add_row([i,self.flume_config[i],status,Channel_put_speed,Channel_take_speed,Sink_sum_speed])
        print table