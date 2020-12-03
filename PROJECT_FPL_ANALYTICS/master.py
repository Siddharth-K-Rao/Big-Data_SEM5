#!/usr/bin/python3
from __future__ import print_function

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
from pyspark.sql import functions as F
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,FloatType
date1=0

def combine(new_list,old_list):
    if not(new_list):
        return old_list
    if not(old_list):
        return new_list
    #print("here ",old_list+new_list)
    return old_list+new_list

def combine_list(new_list,old_list):
    if not(old_list):
        old_list=[]
    if not(new_list):
        return old_list
    old_list.append(new_list)
    x=old_list.copy()
    return x

def combine_events(new_list,old_list):
    if not(old_list):
        old_list=[0 for i in range(15)]
    for i in new_list:
        old_list=[old_list[k]+i[k] for k in range(15)]
    return old_list+[new_list[-1][-1]] if new_list else old_list

def combine_final_events(new_list,old_list):
    if not(old_list):
        old_list=[0 for i in range(10)]
    #print("here",new_list)
    #returns fouls,goals,own_goals,pass metrics,shots metrics
    z=old_list.copy()
    try:
        x=[new_list[0][13],new_list[0][12],new_list[0][14],new_list[0][0],new_list[0][1],new_list[0][2],new_list[0][9],new_list[0][10],new_list[0][11]]
        z=[old_list[i]+x[i] for i in range(9)]+[old_list[-1]+1]
    except:
        pass
    return z

def eventupdate(lines):
    i=json.loads(lines)
    player=(i["playerId"],[0 for i in range(16)])
    #player[1]=[acc_pass,key_pass,tot_pass,dw,dn,dt,acc_fk,penal_scored,total_fk,\
    # stg,st,totalshots,goals,fouls,own_goals,teamID]
    j=i["eventId"]
    tags=[k["id"] for k in i["tags"]]
    player[1][-1]=i["teamId"]
    if 102 in tags:
        player[1][14]+=1
    if 101 in tags:
        player[1][12]+=1
    if j==8:
        if 302 in tags:
            if 1801 in tags:
                player[1][1]+=2
            player[1][2]+=2
        elif 1801 in tags:
            player[1][0]+=1
            player[1][2]+=1
        elif 1802 in tags:
            player[1][2]+=1
        else:
            pass
    elif j==1:
        if 702 in tags:
            player[1][4]+=0.5
        if 703 in tags:
            player[1][3]+=1
        player[1][5]+=1
    elif j==3:
        player[1][8]+=1
        if i["subEventId"]==35 and 101 in tags:
            player[1][7]+=1
        if 1801 in tags:
            player[1][6]+=1   
    elif j==10:
        if 1801 in tags:
            if 101 in tags:
                player[1][9]+=1
            else:
                player[1][10]+=0.5
        player[1][11]+=1
    elif j==2:
        player[1][13]+=1
    return player

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
                try:
                    name=broadcastplayers.value[str(k["playerId"])]
                except:
                    continue
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
                try:
                    name=broadcastplayers.value[str(k["playerId"])]
                except:
                    continue
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
    global date1
    if "wyId" in i:
        #match_init()
        dateutc=i["dateutc"].split(" ")[0]
        date1=dateutc
        print("here",date1)
        label=i["label"]
        yield (str(dateutc)+" "+str(label),matchupdate(i,i["dateutc"].split(" ")[0],i["label"]))
    else:
        pass

'''
def checkevents(lines):
    i=json.loads(lines)
    if "wyId" not in i:#means it is an event
'''

#assumed once a player is subbed in, he is never subbed out
def cal_time(lines):
    j=json.loads(lines)
    di={}
    for i in j["teamsData"]:
        if j["teamsData"][i]["hasFormation"]:
            for k in j["teamsData"][i]["formation"]:#bench/line-up
                if k=="bench":
                    for l in j["teamsData"][i]["formation"][k]:
                        di[l["playerId"]]=(0,0)
                elif k=="lineup":
                    for l in j["teamsData"][i]["formation"][k]:
                        di[l["playerId"]]=(0,90)
            for k in j["teamsData"][i]["formation"]["substitutions"]:
                di[k["playerIn"]]=(k["minute"],90)
                di[k["playerOut"]]=(di[k["playerOut"]][0],k['minute'])
    for i in di:
        if di[i][1]-di[i][0]==90:
            yield(i,1.05)
        else:
            yield(i,(di[i][1]-di[i][0])/90)

def match_calc_full(a,b):
    if not(a) and not(b):
        return []
    if not(a):
        a=[0 for i in range(16)]
        a[-1]=b[-1]
    if not(b):
        b=[0 for i in range(16)]
        b[-1]=a[-1]
    a=[a[i]+b[i] for i in range(15)]+[a[-1]]
    #print(a)
    return a

def stat_calculator(z):
    if not(z):
        return []
    #print("here",z)
    a=float(z[1][0])
    k=z[1][1]
    k[-1]=int(k[-1])
    u,v,w,x=0,0,0,0
    #print("here",k,a)
    try:
        u=(k[0]+k[1])/k[2]#pass accuracy
    except:
        pass
    try:
        v=(k[3]+k[4])/k[5]#duel
    except:
        pass
    try:
        w=(k[6]+k[7])/k[8]#free_kick
    except:
        pass
    try:
        x=(k[9]+k[10])/k[11]#shots on target
    except:
        pass
    player_contrib=((u+v+w+x)/4)*a
    player_contrib=player_contrib-(0.05*k[13]+0.5*k[14])*player_contrib
    #print(player_contrib,k[15])
    return (z[0],player_contrib,k[15])

def change_rating(new_list,old_list):
    if not(new_list):
        return old_list
    if not(old_list):
        old_list=(0.5,0)
    k=(old_list[0]+new_list[0][0])/2
    #print(k)
    return (k,k-old_list[0],new_list[0][1])

def get_date(lines):
    x=json.loads(lines)
    date=x["dateutc"].split(" ")[0]
    teams=list(x['teamsData'].keys())
    for i in teams:
        yield (int(i),date)

def final_comb(lines):
    #print(lines)
    if not(lines):
        return []
    return (lines[0],lines[1][1])

#starting the spark sesstion
conf=SparkConf()
conf.setAppName('BD_FPL_Project')
spark=SparkSession.builder.appName("FPL_analytics").getOrCreate()

#reading the csv of all players
playercsvt=spark.read.option('header',True).csv("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/players.csv")
playercsv=playercsvt.select("Id","name").rdd.collectAsMap()#makes a dictionary of all players

sc = spark.sparkContext
broadcastplayers=sc.broadcast(playercsv)#available to all workers

ssc=StreamingContext(sc,24) 
ssc.checkpoint('checkpoint_FPL')
lines = ssc.socketTextStream("localhost", 6100)
#for matches
matchdata=lines.filter(lambda x:True if "wyId" in json.loads(x) else False)
#matchdata.pprint()

#gives the info about a match
matches=matchdata.flatMap(checkmatches)
#matches.pprint()

#get teamID,date for regression
datematches=matchdata.flatMap(get_date)
#datematches.pprint()

#to store all the matches info
allmatches=matches.updateStateByKey(combine)
#allmatches.pprint()

def make_str(lines):
    label=(lines[0]+';'+str(lines[1]).strip("[]")).replace("'",'"')
    return label
allmatches1=allmatches.map(make_str)

#players on field time
times=matchdata.flatMap(cal_time)
#times.pprint()

#all the events filtered
events=lines.filter(lambda x:True if "wyId" not in json.loads(x) else False)
#events.pprint()

#based on the event return the stat for a player for that event
playermatchstats=events.map(eventupdate)
#playermatchstats.pprint()

#combines all events for each player
playermatchstats=playermatchstats.reduceByKey(match_calc_full)
#playermatchstats.pprint()

#normalised performance
newtimes=times.join(playermatchstats)
#newtimes.pprint()

#matchevents return playerID,[normalised_player_performance,teamID] for a match
matchevents=newtimes.map(stat_calculator)#gives the performance in a match and team 
matchevents=matchevents.map(lambda x:(x[0],[x[1],x[2]]))
#matchevents.pprint()

#for all the matches how a player has performed
#returns (new_rating,change_in_rating,player_ID)
change_in_player_ratings=matchevents.updateStateByKey(change_rating)
#change_in_player_ratings.pprint()

#for pass_accuracy calculation because they can be determined from past status
allmatchevents=playermatchstats.updateStateByKey(combine_final_events)
#allmatchevents.pprint()

#all the metrics -> of the current match including change in ratings
particular_match_change=matchevents.join(change_in_player_ratings)
particular_match_change=particular_match_change.map(final_comb)
#particular_match_change.pprint()

#returns teamID,[playerID,changeInRating]
particular_rate_change=particular_match_change.map(lambda x:(x[1][2],[x[0],x[1][0]]))

rate_date_change=particular_rate_change.join(datematches)
rate_date_change=rate_date_change.map(lambda x:(x[1][0][0],(x[1][1],x[1][0][-1])))
rate_date_change=rate_date_change.updateStateByKey(combine_list)
#rate_date_change.pprint()

particular_rate_change=particular_match_change.map(lambda x:(x[1][2],[x[0],x[1][1]]))
#particular_rate_change.pprint()

def cart_prod(rdd_a,rdd_b):
    rdd_c=rdd_a.cartesian(rdd_b)
    return rdd_c

def calcoeff(lines):
    play1=lines[0]
    play2=lines[1]
    if play1[0]==play2[0]:
        if (play1[1][1]<0 and play2[1][1]<0):
            yield((play1[1][0],play2[1][0]),(play1[1][1]+play2[1][1])*(-0.5))
        elif (play1[1][1]>0 and play2[1][1]>0):
            yield((play1[1][0],play2[1][0]),(play1[1][1]+play2[1][1])*0.5)
        else:
            yield((play1[1][0],play2[1][0]),-0.5*abs(play1[1][1]-play2[1][1]))
    else:
        if (play1[1][1]<0 and play2[1][1]<0):
            yield((play1[1][0],play2[1][0]),(play1[1][1]-play2[1][1])*(0.5))
        elif (play1[1][1]>0 and play2[1][1]>0):
            yield((play1[1][0],play2[1][0]),(play1[1][1]-play2[1][1])*(-0.5))
        else:
            yield((play1[1][0],play2[1][0]),0.5*abs(play1[1][1]-play2[1][1]))

def chem_cal(new_list,old_list):
    if not(old_list):
        old_list=0.5
    if not(new_list):
        return old_list
    #print("here",old_list,new_list)
    return old_list+new_list[0]

cart_prod_dstream=particular_rate_change.transformWith(cart_prod,particular_rate_change)
cart_prod_dstream=cart_prod_dstream.filter(lambda x:x[0][1][0]!=x[1][1][0])
cart_prod_dstream=cart_prod_dstream.flatMap(calcoeff)
#cart_prod_dstream.pprint()

chem_coeff=cart_prod_dstream.updateStateByKey(chem_cal)
#chem_coeff.pprint()

particular_rate_change=particular_match_change.map(lambda x:str((x[0],x[1][0])).strip("()"))
#particular_rate_change.pprint()

#id,date,rating
def cal_karo_ji(lines):
    for i in lines[1]:
        tup=i[0]
        yield(str(lines[0])+","+tup[0]+","+str(tup[1]))

rate_date_change=rate_date_change.flatMap(cal_karo_ji)
rate_date_change.pprint()

allmatches1.repartition(1).saveAsTextFiles("file:///home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/matchdata/matchinfo","txt")
chem_coeff.map(lambda x:str(x[0][0])+";"+str(x[0][1])+","+str(x[1])).repartition(1).saveAsTextFiles("file:///home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/chem/chemdata","txt")
allmatchevents.map(lambda x:str(x).strip("[]()").replace("[","")).repartition(1).saveAsTextFiles("file:///home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/playerdata/playerinfo","txt")
particular_rate_change.repartition(1).saveAsTextFiles("file:///home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/playerrank/rating","txt")
rate_date_change.repartition(1).saveAsTextFiles("file:///home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/playerreg/players","txt")

ssc.start()
ssc.awaitTermination(800)#giving some extra time for computations
ssc.stop()

'''
#print(playercsv) dictionary with all players
#sc=SparkContext.getOrCreate(conf=conf)
#matchupdate({},"2017-08-11","Arsenal - Leicester City, 4 - 3")
#all events of a particular match will be streamed together
'''
'''
if (play1[1][1]<0 and play2[1][1]<0):
            yield((play1[1][0],play2[1][0]),(play1[1][1]+play2[1][1])*(-0.5))
        elif (play1[1][1]>0 and play2[1][1]>0):
            yield((play1[1][0],play2[1][0]),(play1[1][1]+play2[1][1])*0.5)
        else:
            yield((play1[1][0],play2[1][0]),-0.5*(play1[1][1]-play2[1][1]))
'''
'''

'''
