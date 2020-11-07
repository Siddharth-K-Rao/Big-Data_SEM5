#!/usr/bin/python3
import os
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("YoYo").getOrCreate()

if len(sys.argv) != 5:
        #print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

shape_file=spark.read.csv(sys.argv[3],header=True)
shape_stat_file=spark.read.csv(sys.argv[4],header=True)
word=sys.argv[1]
k=int(sys.argv[2])
shape_file=shape_file.filter(shape_file.word==word)

final_csv=shape_file.join(shape_stat_file,shape_file.key_id==shape_stat_file.key_id)
final_csv=final_csv.filter((final_csv.recognized==False) & (final_csv.Total_Strokes<k))#we need to make separate parentheses for each condition and combining operator is '&' and not 'and'
#print(final_csv.head(5))

final_csv=final_csv.groupby(final_csv.countrycode).count().orderBy(final_csv.countrycode)
flag=1
for i in final_csv.collect():
	flag=0
	countrycode,count=i.__getitem__('countrycode'),i.__getitem__('count')
	print(countrycode,count,sep=",")
if flag:
        print(0)

