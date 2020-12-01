#from pyspark.mllib.clustering import KMeans, KMeansModel
import numpy
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SQLContext
from pyspark.ml.regression import LinearRegression
#from pyspark.ml.evaluation import RegressionEvaluator
from datetime import date
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("FPL_ui").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
import json


#player_profile -> dataframe
def cluster(player_profile):
    df=player_profile
    FEATURES_COL = ['fouls','goals','own_goals','pass1','pass2','pass3','st1','st2','st3']
    for col in df.columns:
	    if col in FEATURES_COL:
	        df = df.withColumn(col,df[col].cast('float'))
    df = df.na.drop()
    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    df_kmeans = vecAssembler.transform(df).select('Id', 'features')
    k = 5
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(df_kmeans)
    centers = model.clusterCenters()
    transformed = model.transform(df_kmeans).select('Id', 'prediction')
    rows = transformed.collect()
    df_pred = sqlContext.createDataFrame(rows)
    df_pred = df_pred.join(df, 'Id')

    ratings=df_pred.groupby('prediction').agg({'player_rating':'mean'}).withColumnRenamed('avg(player_rating)','avg_player_rating').select("prediction","avg_player_rating")
    #chemistry=df_pred.groupby('prediction').agg({'chem_coeff':'mean'}).withColumnRenamed('avg(chem_coeff)','avg_chem_coeff').select("prediction","avg_chem_coeff")

    ratings_di={}
    chemistry_di={}

    for i in ratings.collect():
	    ratings_di[i._getitem('prediction')] = i.getitem_('avg_player_rating')
    #for i in chemistry.collect():
    #    chemistry_di[i._getitem('prediction')] = i.getitem_('avg_chem_coeff')
    
    #df = df.withColumn("player_rating",when(col("no_of_matches")<5,ratings_di[col("prediction")]).otherwise(col("player_rating")))
    df_pred = df_pred.withColumn("player_rating",when(df.no_of_matches<5,ratings_di[df_pred.prediction]).otherwise(df_pred.player_rating))
    
    #df.withColumn("chem_coeff",when(col("no_of_matches")<5,ratings_di[col("prediction")]).otherwise(col("chem_coeff")))
    return df
	 

def date_diff(d1,d2):
	x = d1.split("-")
	f_date = date(x[0],x[1],x[2])
	y = d1.split("-")
	l_date = date(y[0],y[1],y[2])
	return l_date-f_date


def get_chemistry(key):
    pass


# player_data -> dataframe with player_rating, date
# match_date -> to predict the players rating on match_date
def regression(player_data, match_date):
	first_date = player_data.select("date").collect()[0]
	#x = firstdate.split("-")
	#f_date = date(x[0],x[1],x[2])
	new_data = player_data.withColumn("date",date_diff(first_date,col("date")))
	new_data = new_data.withColumnRenamed("date","age")
	return quad_regression(new_data,match_date)


def quad_regression(player_data,match_date):
	first_date = player_data.select("date").collect()[0]
	age = date_diff(first_date,match_date)
	age_sq = age*age
	test = spark.createDataframe([(age,age_sq,0)],['age','age_sq','label'])

	data2 = player_data.withColumn('age_sq',data.age*data.age)
	data2 = player_data.select(player_data.age,player_data.age_sq,player_data.player_rating.alias('label'))
	assembler = VectorAssembler().setInputCols(['age','age_sq']).setOutputCol('features')
	train = assembler.transform(data2)
	train2 = train.select("features","label")

	lr = LinearRegression()
	model = lr.fit(train2)
	test1 = assembler.transform(test)
	test2 = test1.select('features')
	test3 = model.transform(test2)
	new_rating = test3.select("prediction").collect()[0]
	return new_rating

	
players_csv = spark.read.csv("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/players.csv",header=True)
player_profile = spark.read.csv("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/part-00000",header=True)
id_rating = spark.read.csv("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/playerrank/part-00000",header=True)
player_profile = players_csv.join(player_profile,['Id'])
player_profile = player_profile.join(id_rating,['Id'])
player_profile = cluster(player_profile)

#match_data = sc.textFile("/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/match_data/part-00001")





#team1 and team2 are a list of players id in team 1 and team 2, 
#chemistry -> dict of dict -> {player1:{player2:chemistry,player3:chemistry},....}
#player_profile -> dataframe
#returns (winning chane of team1, winning chance of team2)
def winning_chance(player_profile,chemistry,team1,team2,match_date):
    if(len(team1) < 11 or len(team2) < 11):
        return "Invalid team"
    d = {'GK':0,'DF':0,'MD':0,'FW':0}
    for i in team1:
        d[player_profile.filter(player_profile.Id==i).collect()[0].__getitem('role')]+=1
    if(d['GK']<1 or d['DF']<3 or d['MF']<2 or d['FW']<1):
        return "Invalid team"
    
    d = {'GK':0,'DF':0,'MD':0,'FW':0}
    for i in team1:
        d[player_profile.filter(player_profile.Id==i).collect()[0].__getitem('role')]+=1
    if(d['GK']<1 or d['DF']<3 or d['MF']<2 or d['FW']<1):
        return "Invalid team"

    strength_a = 0
    for i in team1:
        player_strength = 0
        avg_chem = 0
        for j in team1:
            if(i!=j):
                try:
                    avg_chem+=chemistry[i][j]
                except:
                    avg_chem+=0.5

        avg_chem = avg_chem/10
        play_rating = player_profile.filter(player_profile.Id == i).collect()[0]._getitem_('player_rating')
        #use regression to compute actual rating
        #player_data = player_date_rating.filter(player_date_rating.Id == i)
        #play_rating = regression(player_data,match_date)
        player_strength = avg_chem*play_rating
        strength_a += player_strength
    strength_a = strength_a/11

    strength_b = 0
    for i in team2:
        player_strength = 0
        avg_chem = 0
        for j in team2:
            if(i!=j):
                try:
                    avg_chem+=chemistry[i][j]
                except:
                    avg_chem+=0.5
        avg_chem = avg_chem/10
        player_strength = avg_chem*player_profile.filter(player_profile.Id == i).collect()[0]._getitem_('player_rating')
        strength_b += player_strength
    strength_b = strength_b/11
    
    chance_a = (0.5 + strength_a - (strength_a + strength_b)/2)*100
    chance_b = 100 - chance_b
    
    return (chance_a,chance_b)


# line -> request in json format
# returns output json
def query(s):
    d = json.loads(s)
    #d = {'req_type': 2,'name': 'Rob Holding'}
    if 'req_type' in d:
        if d['req_type'] == 1:
            team1 = []
            team2 = []
            date = d['date']
            for i in d['team1']:
                if i != 'name':
                    player_id = player_profile.filter(player_profile.name == d['team1'][i]).select("Id").collect()[0][0]
                    team1.append(player_id)
                else:
                    n1 = d['team1'][i]
            for i in d['team2']:
                if i != 'name':
                    player_id = player_profile.filter(player_profile.name == d['team2'][i]).select("Id").collect()[0][0]
                    team2.append(player_id)#appends name, shd append player id
                else:
                    n2 = d['team2'][i]
            ch_a,ch_b = winning_chance(player_profile,chemistry,team1,team2,date)
            
            out = {"team1":{"name":n1,"winning_chance":ch_a},"team2":{"name":n2},"winning_chance":ch_b}
            
            return json.dumps(out,indent=2)
            
        elif d['req_type'] == 2:
            name = d['name']
            player_id = player_profile.filter(player_profile.name == name).select("Id").collect()[0][0]
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
            out['percentage_pass_accuracy'] = round(((int(player_profile.filter(player_profile.Id==player_id).select("pass1").collect()[0][0])+int(player_profile.filter(player_profile.Id==player_id).select("pass2").collect()[0][0]))/(int(player_profile.filter(player_profile.Id==player_id).select("pass3").collect()[0][0])+0.00001))*100,2)
            out['percent_shots_on_target'] = round(((int(player_profile.filter(player_profile.Id==player_id).select("st1").collect()[0][0])+int(player_profile.filter(player_profile.Id==player_id).select("st2").collect()[0][0]))/(int(player_profile.filter(player_profile.Id==player_id).select("st3").collect()[0][0])+0.00001))*100,2)
            
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
       
s1 = '''{"req_type": 2,"name": "Rob Holding"}'''
s2 = '''{"date":"2017-08-11","label":"Arsenal - Leicester City, 4 - 3"}'''
print(query(s1))
