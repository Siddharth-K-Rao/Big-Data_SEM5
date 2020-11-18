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

match_events='/home/sreyans/Desktop/SEM5/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/match_events.json'
spark=SparkSession.builder.appName("FPL_BUILDER").getOrCreate()
json_df=spark.read.option("multiline",True).json(match_events)
if not(json_df.rdd.isEmpty()):
    print("NO")
    json_df.printSchema()
else:
    print("YES")
try:
    k=json_df["2017-08-11 Arsenal - Leicester City, 4 - 3"]
    print("\nYESSSSS")
except:
    print("\nNO")
json_object=json.dumps(my_dict)
sc=spark.sparkContext
new_dict_df=spark.createDataFrame(dictionary
