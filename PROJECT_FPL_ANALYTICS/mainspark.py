#!/usr/bin/python3
from __future__ import print_function
import findspark
findspark.init()

from operator import add
import os
import json
import requests
import socket
import time
import csv
import sys
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession

def combine(new_list,old_list):
    if not(old_list):
        old_list=[]
    return new_list+old_list
        
def eventupdate(i):
    return

def matchupdate(matchdict,date,label):
    datatuple=str(date)+" "+str(label)
    di={}
    di['date']=date
    di['duration']=matchdict['duration']
    di['gameweek']=matchdict['gameweek']
    di['venue']=matchdict['venue']
    t1,t2=list(matchdict['teamsData'].keys())
    n1,n2=label.split(",")[0].split("-")
    n1=n1.strip()
    n2=n2.strip()
    winner=matchdict['winner']
    if winner=="0":
        di['winner']="NULL"
    else:
        if winner==t1:
            di['winner']=n1
        else:
            di['winner']=n2
    di['goals']=[]
    di['own_goals']=[]
    di['yellow_cards']=[]
    di['red_cards']=[]
    for i in [(t1,n1),(t2,n2)]:
        j=matchdict["teamsData"][i[0]]
        if int(j['hasFormation']):
            for k in j['formation']['bench']:
                #print((k["playerId"]),str(k["playerId"]))
                name=broadcastplayers.value[str(k["playerId"])]
                #print(name)
                if int(k["ownGoals"])>0:
                    dic={"name":name,"team":i[1],"number_of_own_goals":k["ownGoals"]}
                    di['own_goals'].append(dic)
                if int(k["goals"])>0:
                    dic={"name":name,"team":i[1],"number_of_goals":k["goals"]}  
                    di["goals"].append(dic)
                if int(k['redCards']):
                    di['red_cards'].append(name)
                if int(k['yellowCards']):
                    di['yellow_cards'].append(name)
            for k in j['formation']['lineup']:
                #print((k["playerId"]),str(k["playerId"]))
                name=broadcastplayers.value[str(k["playerId"])]
                if int(k["ownGoals"])>0:
                    dic={"name":name,"team":i[1],"number_of_own_goals":k["ownGoals"]}
                    di['own_goals'].append(dic)
                if int(k["goals"])>0:
                    dic={"name":name,"team":i[1],"number_of_goals":k["goals"]}  
                    di["goals"].append(dic)
                if int(k['redCards']):
                    di['red_cards'].append(name)
                if int(k['yellowCards']):
                    di['yellow_cards'].append(name)
    return di


def checkmatches(lines): 
    i=json.loads(lines)
    if "wyId" in i:
        dateutc=i["dateutc"].split(" ")[0]
        label=i["label"]
        yield ("matches",{str(dateutc)+" "+str(label):matchupdate(i,i["dateutc"].split(" ")[0],i["label"])})
    else:
        yield ("events",lines)


conf=SparkConf()
conf.setAppName('BD_FPL_Project')
spark=SparkSession.builder.appName("FPL_analytics").getOrCreate()

playercsv=spark.read.option('header',True).csv("/home/sreyans/Desktop/SEM5/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/players.csv")
playercsv=playercsv.select("Id","name").rdd.collectAsMap()#makes a dictionary of all players


sc = spark.sparkContext
broadcastplayers=sc.broadcast(playercsv)#available to all workers
#print(broadcastplayers.value["20612"])

ssc=StreamingContext(sc,8) #5s for time difference between two batches - design decision
ssc.checkpoint('checkpoint_FPL')
lines = ssc.socketTextStream("localhost", 6100)

matches=lines.flatMap(checkmatches)
#matches.pprint()
matches.map(lambda x:(x[0],x[1]))
matches=matches.groupByKey().mapValues(list)
matches.pprint(1)
#print(matches.keys())
allmatches=matches.updateStateByKey(combine)
allmatches=allmatches.filter(lambda x:x[1] if x[0]=='matches' else "")
allmatches.pprint()
#print(allmatches.value['matches'])

#lines.foreachRDD(splitrdd)
ssc.start()
ssc.awaitTermination(20)#giving soome extra time for computations
ssc.stop()

'''
#print(playercsv) dictionary with all players
#sc=SparkContext.getOrCreate(conf=conf)
#matchupdate({},"2017-08-11","Arsenal - Leicester City, 4 - 3")
#all events of a particular match will be streamed together
'''
