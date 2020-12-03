#!/usr/bin/python3

import numpy
import sys
import json
import datetime as dt
from pyspark.ml.clustering import KMeans,KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SQLContext
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,FloatType

#creation of spark session and spark context for ML parts
spark = SparkSession.builder.appName("FPL_ML").getOrCreate()
sc=spark.sparkContext
sqlContext = SQLContext(sc)

#store the average rating of a cluster of players
ratings_di={}

#player_profile -> dataframe
def cluster(player_profile):
    df=player_profile
    #columns used for clustering -> features
    FEATURES_COL = ['fouls','goals','own_goals','pass1','pass2','pass3','st1','st2','st3']
    for col in df.columns:
        if col in FEATURES_COL:#converts all feature_cols to float datatype
            df = df.withColumn(col,df[col].cast('float'))
    df = df.na.drop()
    
    #combines features columns to make a single feature vector
    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    df_kmeans = vecAssembler.transform(df).select('Id', 'features')
    
    k = 5#number of clusters
    #we have set maxIters to 3 so that processing is faster, otherwise VM crashed
    kmeans = KMeans().setK(k).setMaxIter(3).setSeed(1).setFeaturesCol("features")
    
    #seeing if a model exists, then we use that.Otherwise make a new model in except and save it
    model_path = "/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/"+"kmeans_model"
    try:
        model = KMeansModel.load(model_path)
        #print("loading saved model")
    except:
        model = kmeans.fit(df_kmeans)#this is the time consuming part
        model.save(model_path)
    
    centers = model.clusterCenters()#centroid of each cluster
    transformed = model.transform(df_kmeans).select('Id', 'prediction')#applies our model to the given data
    rows = transformed.collect()
    df_pred = sqlContext.createDataFrame(rows)
    df_pred = df_pred.join(df, 'Id')
    
    
    #gives a mean rating for each cluster of players
    ratings=df_pred.groupby('prediction').agg({'player_rating':'mean'}).withColumnRenamed('avg(player_rating)','avg_player_rating').select("prediction","avg_player_rating")
    
    for i in ratings.collect():
        ratings_di[i.__getitem__('prediction')] = i.__getitem__('avg_player_rating')

    df_pred = df_pred.withColumn("player_rating",when(df_pred.no_of_matches<5,udf_func(df_pred.prediction)).otherwise(df_pred.player_rating))
    
    #returns the ratings
    #print(df_pred.show())
    return df_pred

	 
def new_rating(x):
    return ratings_di[x]

#user-defined function for making a new column from an old one    
udf_func = udf(new_rating,StringType())    	 	 


def date_diff(d1,d2):
    d1 = dt.datetime.strptime(d1, "%Y-%m-%d")
    d2 = dt.datetime.strptime(d2, "%Y-%m-%d")
    diff = (d2 - d1).days
    return str(diff)

#user-defined function for making a new column from an old one	
udf2 = udf(date_diff,StringType())


# player_data -> dataframe with player_rating, date
# match_date -> to predict the players rating on match_date
def regression(player_data, match_date):
    birth_date = player_data.select("date").collect()[0][0]
    new_data = player_data.withColumn("birth_date",when(player_data.date != "",str(birth_date)).otherwise(player_data.date))
    new_data = new_data.withColumn("date",udf2(new_data.birth_date,new_data.date))
    new_data = new_data.withColumnRenamed("date","age")
    new_val = quad_regression(new_data,match_date)
    #print("return in regression",new_val)
    return new_val


def quad_regression(player_data,match_date):
    birth_date = player_data.select("birth_date").collect()[0][0]
    age = int(date_diff(str(birth_date),match_date))
    age_sq = age*age
    test = sqlContext.createDataFrame([(age,age_sq,0)],['age','age_sq','label'])
    data2 = player_data.withColumn('age_sq',player_data.age*player_data.age)
    data2 = data2.select(data2.age,data2.age_sq,data2.player_rating.alias('label'))

    FEATURES_COL = ['age','age_sq','label']
    for col in data2.columns:
        if col in FEATURES_COL:
            data2 = data2.withColumn(col,data2[col].cast('float'))


    assembler = VectorAssembler().setInputCols(['age','age_sq']).setOutputCol('features')
    train = assembler.transform(data2)
    train2 = train.select("features","label")

    lr = LinearRegression()#age_squared is the main factor
    model = lr.fit(train2)#training the model
    test1 = assembler.transform(test)
    test2 = test1.select('features','label')
    test3 = model.transform(test2)
    new_rating = test3.select("prediction").collect()[0][0]
    
    #print("return in quad_regression",new_rating)
    if new_rating>1:
        return 0.99
    elif new_rating<1:
        return 0.2
    else:
        return new_rating    


def get_chemistry(id1,id2):
    key = str(id1)+";"+str(id2)
    try:
        chemistry = chem.filter(chem.key == key).select("chemistry_coeff").collect()[0][0]
        #print(chemistry)
        #return float(chemistry)
    except:
        chemistry = 0.5
    return float(chemistry)


#schemas for defining the various data files that we have
chemi=[StructField('key',StringType(),False),StructField('chemistry_coeff',StringType(),False)]
chem_struct=StructType(fields=chemi)

player=[StructField("Id",StringType(),False),StructField("fouls",StringType(),False),\
    StructField("goals",StringType(),False),StructField("own_goals",StringType(),False),\
        StructField("pass1",StringType(),False),StructField("pass2",StringType(),False),StructField("pass3",StringType(),False),\
            StructField("st1",StringType(),False),StructField("st2",StringType(),False),StructField("st3",StringType(),False),StructField("no_of_matches",StringType(),False)]
player_struct=StructType(fields=player)

idr=[StructField('Id',StringType(),False),StructField('player_rating',StringType(),False)]
idstruct=StructType(fields=idr)

pdr = [StructField('Id',StringType(),False),StructField('date',StringType(),False),StructField('player_rating',StringType(),False)]
pdrstruct = StructType(fields=pdr)

players_csv = spark.read.csv("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/players.csv",header=True)#player data like name id birthdate...
player_info = spark.read.csv("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerdata/playerinfo/part-00000",schema=player_struct,header=False)#has fouls goals etc
id_rating = spark.read.csv("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerrank/rating/part-00000",schema=idstruct,header=False)# has Id,player_rating
chem = spark.read.csv("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/chem/chemdata/part-00000",schema=chem_struct,header=False)# player1Id;player2Id,chemistry
match_data = sc.textFile("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/matchdata/matchinfo/part-00000")
player_date_rating = spark.read.csv("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerreg/players/part-00000",schema=pdrstruct,header=False)


player_info = player_info.join(id_rating,['Id']) #has Id,rating,fouls,goals etc
player_profile = players_csv.join(player_info,['Id']) #join of everything

player_profile2 = players_csv.join(player_info,['Id'],how='full')
player_profile2 = player_profile2.na.fill(value="0")
player_profile2 = player_profile2.withColumn("player_rating",when(player_profile2.player_rating == "0","0.5").otherwise(player_profile2.player_rating))
#print(player_profile2.show())


player_profile = cluster(player_profile)


#team1 and team2 are a list of players id in team 1 and team 2, 
#chemistry -> dict of dict -> {player1:{player2:chemistry,player3:chemistry},....}
#player_profile -> dataframe
#returns (winning chane of team1, winning chance of team2)
def winning_chance(player_profile,team1,team2,match_date):
    if(len(team1) < 11 or len(team2) < 11):
        return "Invalid team"
    
    d = {'GK':0,'DF':0,'MD':0,'FW':0}
    for i in team1:
        try:
            d[player_profile.filter(player_profile.Id==i).select('role').collect()[0][0]] += 1#.__getitem__('role')]+=1
        except:    
            try:
                d[player_profile2.filter(player_profile2.Id==i).select('role').collect()[0][0]] += 1#.__getitem__('role')]+=1
            except Exception as e:
                return "Invalid team"
    if(d['GK']<1 or d['DF']<3 or d['MD']<2 or d['FW']<1):
        return "Invalid team"
    
    d = {'GK':0,'DF':0,'MD':0,'FW':0}
    for i in team1:
        try:
            d[player_profile.filter(player_profile.Id==i).select('role').collect()[0][0]] += 1#.__getitem__('role')]+=1            
        except:    
            try:
                d[player_profile2.filter(player_profile2.Id==i).select('role').collect()[0][0]] += 1#.__getitem__('role')]+=1
            except:
                return "Invalid team"
    if(d['GK']!=1 or d['DF']<3 or d['MD']<2 or d['FW']<1):
        return "Invalid team"

    strength_a = 0
    for i in team1:
        player_strength = 0
        avg_chem = 0
        for j in team1:
            if(i!=j):
                try:
                	avg_chem += get_chemistry(i,j)
                except:
                    avg_chem += 0.5

        avg_chem = avg_chem/10
        #play_rating = player_profile2.filter(player_profile2.Id == i).select('player_rating').collect()[0][0]
        #use regression to predict the player rating on the given date
        try:
            player_data = player_date_rating.filter(player_date_rating.Id == i)
            play_rating = regression(player_data,match_date)
        except:
            try:
                play_rating = player_profile.filter(player_profile.Id == i).select('player_rating').collect()[0][0]
            except:
                play_rating = player_profile2.filter(player_profile2.Id == i).select('player_rating').collect()[0][0]
        
        if float(play_rating) < 0.2:
            return ("Retired Player")

        player_strength = float(avg_chem)*float(play_rating)
        
        strength_a += player_strength
    strength_a = strength_a/11

    strength_b = 0
    for i in team2:
        player_strength = 0
        avg_chem = 0
        for j in team2:
            if(i!=j):
                try:
                    avg_chem += get_chemistry(i,j)
                except:
                    avg_chem += 0.5
        avg_chem = avg_chem/10
    
        #play_rating = player_profile2.filter(player_profile2.Id == i).select('player_rating').collect()[0][0]
        #use regression to predict the player rating on the given date
        try:
            player_data = player_date_rating.filter(player_date_rating.Id == i)
            play_rating = regression(player_data,match_date)
        except:
            try:
                play_rating = player_profile.filter(player_profile.Id == i).select('player_rating').collect()[0][0]
            except:
                play_rating = player_profile2.filter(player_profile2.Id == i).select('player_rating').collect()[0][0]

        if float(play_rating) < 0.2:
            return ("Retired Player")
        
        player_strength = float(avg_chem)*float(play_rating)
        
        strength_b += player_strength
    strength_b = strength_b/11
    
    chance_a = int((0.5 + strength_a - (strength_a + strength_b)/2)*100)
    chance_b = 100 - chance_a
    
    return (chance_a,chance_b)


# line -> request in json format
# returns output json
def query(s):
    s = s.replace("\\u","\\\\u")
    d = json.loads(s)
    
    if 'req_type' in d:
        if d['req_type'] == 1:#predictions here#d is the dictionary input file
            team1 = []
            team2 = []
            date = d['date']
            for i in d['team1']:
                if i != 'name':               
                    try:
                        #player_id = player_profile.filter(player_profile.name == d['team1'][i]).select("Id").collect()[0][0]
                        player_id = players_csv.filter(players_csv.name == d['team1'][i]).select("Id").collect()[0][0]                   
                        team1.append(player_id)
                    except Exception as e:                       
                        pass
                else:
                    n1 = d['team1'][i]
            for i in d['team2']:
                if i != 'name':
                    #player_id = player_profile.filter(player_profile.name == d['team2'][i]).select("Id").collect()[0][0]
                    player_id = players_csv.filter(players_csv.name == d['team2'][i]).select("Id").collect()[0][0]
                    team2.append(player_id)
                else:
                    n2 = d['team2'][i]
            k = winning_chance(player_profile,team1,team2,date)
            if k=="Invalid team":
                out={"error":k}
                return json.dumps(out,indent=2)
            elif k == "Retired Player":
                out={"error":k}
                return json.dumps(out,indent=2)
            ch_a,ch_b=k[0],k[1]
            out = {"team1":{"name":n1,"winning_chance":ch_a},"team2":{"name":n2,"winning_chance":ch_b}}
            return json.dumps(out,indent=2)
            
        elif d['req_type'] == 2:
            name = d['name']
            try:
                player_id = player_profile.filter(player_profile.name == name).select("Id").collect()[0][0]
            except:
                return json.dumps({"error":"Invalid Player"})
            try:
                player_profile.filter(player_profile.Id==player_id).select("fouls").collect()[0][0]
            except:
                return json.dumps({"error":"Invalid Player"})
            out = {}
            out['name'] = name
            out['birthArea'] = player_profile.filter(player_profile.Id==player_id).select("birthArea").collect()[0][0]
            out['birthDate'] = player_profile.filter(player_profile.Id==player_id).select("birthDate").collect()[0][0]
            out['foot'] = player_profile.filter(player_profile.Id==player_id).select("foot").collect()[0][0]
            out['role'] = player_profile.filter(player_profile.Id==player_id).select("role").collect()[0][0]
            out['height'] = player_profile.filter(player_profile.Id==player_id).select("height").collect()[0][0]
            out['passportArea'] = player_profile.filter(player_profile.Id==player_id).select("passportArea").collect()[0][0]
            out['weight'] = player_profile.filter(player_profile.Id==player_id).select("weight").collect()[0][0]
            out['fouls'] = player_profile.filter(player_profile.Id==player_id).select("fouls").collect()[0][0]
            out['goals'] = player_profile.filter(player_profile.Id==player_id).select("goals").collect()[0][0]
            out['own_goals'] = player_profile.filter(player_profile.Id==player_id).select("own_goals").collect()[0][0]
            
            out['percentage_pass_accuracy'] = int(((int(player_profile.filter(player_profile.Id==player_id).select("pass1").collect()[0][0])+int(player_profile.filter(player_profile.Id==player_id).select("pass2").collect()[0][0]))/(int(player_profile.filter(player_profile.Id==player_id).select("pass3").collect()[0][0])+0.00001))*100)
            out['percent_shots_on_target'] = int(((int(player_profile.filter(player_profile.Id==player_id).select("st1").collect()[0][0])+int(player_profile.filter(player_profile.Id==player_id).select("st2").collect()[0][0]))/(int(player_profile.filter(player_profile.Id==player_id).select("st3").collect()[0][0])+0.00001))*100)
            
            return json.dumps(out,indent=2)
            
    else:#match info needs to be returned
        date = d['date']
        label = d['label']
        key = date+" "+label
        lines = match_data.collect()
        for line in lines:
            if key in line:
                res = line.split(";")[1]
                out = json.loads(res)
        return json.dumps(out,indent=2)


#for connecting to the ui.py file for running the required functions for the tasks
inpf = open(sys.argv[1],"r")
playertext = inpf.read()
ans = query(playertext)
outf = open(sys.argv[2],"w")
outf.write(ans)
inpf.close()
outf.close()




"""
s1 = '''{"req_type": 2,"name": "Chris Brunt"}'''
s2 = '''{"date":"2017-08-11","label":"Arsenal - Leicester City, 4 - 3"}'''
s3 = '''
{"req_type": 1,
"date":"2018-10-18",
"team1":
{"name":"MI","player1":"Erwin Mulder","player2":"Terence Kongolo","player3":"Bruno Martins Indi","player4":"Daryl Janmaat","player5":"Rajiv van La Parra","player6":"Luciano Narsingh","player7":"Nacer Chadli","player8":"Leroy Fer","player9":"Wilfried Guemiand Bony","player10":"Mike van der Hoorn","player11":"Rhu-endly Martina"},
"team2":
{"name":"MI","player1":"Erwin Mulder","player2":"Terence Kongolo","player3":"Bruno Martins Indi","player4":"Daryl Janmaat","player5":"Rajiv van La Parra","player6":"Luciano Narsingh","player7":"Nacer Chadli","player8":"Leroy Fer","player9":"Wilfried Guemiand Bony","player10":"Mike van der Hoorn","player11":"Rhu-endly Martina"}
}'''

s4 = '''
{"req_type": 1,
"date":"2018-10-18",
"team1":
{"name":"MI","player1":"Olivier Giroud","player2":"Aaron Ramsey","player3":"Rob Holding","player4":"Alex Oxlade-Chamberlain","player5":"Ignacio Monreal Eraso","player6":"Petr \u010cech","player7":"Granit Xhaka","player8":"","player9":"Theo  Walcott","player10":"Mohamed Naser Elsayed Elneny","player11":"Daniel Nii Tackie Mensah Welbeck","player12":"Sead Kola\u0161inac","player13":"Alexandre Lacazette","player14":"H\u00e9ctor Beller\u00edn Moruno","player15":"Mesut \u00d6zil"},
"team2":
{"name":"MI","player1":"Kelechi Promise Iheanacho","player2":"Daniel Amartey","player3":"Demarai Gray","player4":"Matty James","player5":"Kasper Schmeichel","player6":"Onyinye Wilfred Ndidi","player7":"Riyad Mahrez","player8":"Shinji Okazaki","player9":"Marc Albrighton","player10":"Harry  Maguire","player11":"Danny Simpson","player12":"Christian Fuchs","player13":"Jamie Vardy"}
}'''
"""

'''
Toby Alderweireld
Daley Blind
Jan Vertonghen
Christian  Dannemann Eriksen
Davy Klaassen
Niki M\u00e4enp\u00e4\u00e4
Ragnar Klavan
Johann  Berg Gu\u00f0munds\u00adson
Erik Pieters
Georginio Wijnaldum
J\u00fcrgen Locadia
'''

'''
Erwin Mulder
Terence Kongolo
Bruno Martins Indi
Daryl Janmaat
Rajiv van La Parra
Luciano Narsingh
Nacer Chadli
Leroy Fer
Wilfried Guemiand Bony
Mike van der Hoorn
Rhu-endly Martina
'''


'''
f1 = open("input.txt",'r')
s = f1.read()

output = query(s)
print(output)
f2 = open("output.txt",'w')
f2.write(output)

f1.close()
f2.close()
'''
#for testing regression
'''
player_data = player_date_rating.filter(player_date_rating.Id == 7868)
play_rating = regression(player_data,"2018-10-12")
print("rreg rateing----",play_rating)
'''
