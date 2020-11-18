#!/usr/bin/python3
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

def eventupdate(i):
    return

def matchupdate(matchdict,date,label):
    print("\n\nMATCHUPDATE\n\n")
    match_events='/home/sreyans/Desktop/SEM5/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/match_events.json'
    datatuple=str(date)+" "+str(label)
    try:
        spark=SparkSession.builder.appName("FPL_BUILDER").getOrCreate()
        json_df=spark.read.json(match_events)
        json_df.printSchema()
    except:
        pass
    '''
    except:
        fulldata={}
    fulldata[datatuple]={}
    fulldata[datatuple]['date']=date
    fulldata[datatuple]['duration']=matchdict['duration']
    fulldata[datatuple]['gameweek']=matchdict['gameweek']
    fulldata[datatuple]['venue']=matchdict['venue']
    t1,t2=list(matchdict['teamsData'].keys())
    n1,n2=label.split(",")[0].split("-")
    n1=n1.strip()
    n2=n2.strip()
    winner=matchdict['winner']
    if winner=="0":
        fulldata[datatuple]['winner']="NULL"
    else:
        if winner==t1:
            fulldata[datatuple]['winner']=n1
        else:
            fulldata[datatuple]['winner']=n2
    fulldata[datatuple]['goals']=[]
    fulldata[datatuple]['own_goals']=[]
    fulldata[datatuple]['yellow_cards']=[]
    fulldata[datatuple]['red_cards']=[]
    
    for i in [(t1,n1),(t2,n2)]:
            j=matchdict["teamsData"][i[0]]
            print("\n\n",j,"\n\n")
            if int(j['hasFormation']):

                for k in j['formation']['bench']:
                    if int(k["ownGoals"])>0:
                        di={"name_id":k["playerId"],"team":i[1],"number_of_goals":k["ownGoals"]}
                        fulldata[datatuple]['own_goals'].append(di)
                    if int(k["goals"])>0:
                        di={"name_id":k["playerId"],"team":i[1],"number_of_goals":k["goals"]}
                        fulldata[datatuple]['goals'].append(di)
                    if int(k['redCards']):
                        fulldata[datatuple]['red_cards'].append(k["playerId"])
                    if int(k['yellowCards']):
                        fulldata[datatuple]['yellow_cards'].append(k["playerId"])

                for k in j['formation']['lineup']:
                    if int(k["ownGoals"])>0:
                        di={"name_id":k["playerId"],"team":i[1],"number_of_goals":k["ownGoals"]}
                        fulldata[datatuple]['own_goals'].append(di)
                    if int(k["goals"])>0:
                        di={"name_id":k["playerId"],"team":i[1],"number_of_goals":k["goals"]}
                        fulldata[datatuple]['goals'].append(di)
                    if int(k['redCards']):
                        fulldata[datatuple]['red_cards'].append(k["playerId"])
                    if int(k['yellowCards']):
                        fulldata[datatuple]['yellow_cards'].append(k["playerId"])

    json_file=open(match_events,"w")
    finalobj=json.dumps(fulldata,indent=2)
    json_file.write(finalobj)
    '''
def splitandcheck(lines):
    print("\n\n",lines) 
    i=json.loads(lines)
    if "wyId" in i:
        print("\n\nHERE\n\n")
        matchupdate(i,i["dateutc"].split(" ")[0],i["label"])
    else:
        eventupdate(i)


def splitrdd(records):
    if (records.isEmpty()):
        return
    records.foreach(splitandcheck)

def printkaro(records):
    print("\n\n",records)  

'''
#native python code ->to preprocess all the players before any match starts

with open('playerprofile.csv', 'w', newline='') as outcsv:
    writer = csv.writer(outcsv)
    writer.writerow(['name','birthArea','birthDate','foot','role','height','passportArea','weight','fouls','goals','own_goals','percent_pass_accuracy','percent_shots_on_target','matches_played'])

    with open('/home/sreyans/Desktop/SEM5/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/play.csv', 'r', newline='') as incsv:
        reader = csv.reader(incsv)
        writer.writerows(row + [0,0,0,0,0,0] for row in reader if "name" not in row)
'''
conf=SparkConf()
conf.setAppName('BD_FPL_Project')
sc=SparkContext.getOrCreate(conf=conf)

ssc=StreamingContext(sc,1) #5s for time difference between two batches - design decision
ssc.checkpoint('checkpoint_FPL')

lines = ssc.socketTextStream("localhost", 6100)
#lines.pprint()
lines.foreachRDD(splitrdd)


ssc.start()
ssc.awaitTermination(100)
ssc.stop()

'''
sc = SparkContext("local[1]", "NetworkFPL") #2 represents number of threads
ssc = StreamingContext(sc, 5) #1 represents the batch difference
lines = ssc.socketTextStream("localhost", 6100)
lines.reduce(add).pprint()
ssc.start()
ssc.awaitTermination()
'''










'''
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
sc = SparkContext("local[2]", "NetworkWordCount") #2 represents number of threads
ssc = StreamingContext(sc, 1) #1 represents the batch difference
lines = ssc.socketTextStream("localhost", 9999)
ssc.start()             # Start the computation
ssc.awaitTermination()
'''
