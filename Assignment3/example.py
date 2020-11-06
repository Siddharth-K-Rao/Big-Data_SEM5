#!/usr/bin/python3
#MADE FOR PERSONAL USE
import os
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("YoYo").getOrCreate()

if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

csv_file=spark.read.csv(sys.argv[1],header=True)
word=sys.argv[2]
#csv_file.printSchema()
#final_csv=csv_file.filter(csv_file.word==word).groupby(csv_file.recognized)
#print(final_csv.count().show())

final_csv=csv_file.filter(csv_file.word==word)
#bool_csv=final_csv.groupby('recognized').count()
final_csv=final_csv.groupby('recognized').agg({'Total_Strokes':'mean'}).withColumnRenamed('avg(Total_Strokes)','average')
#final_csv=draw_csv.join(bool_csv,draw_csv.recognized==bool_csv.recognized)
#print(final_csv.show())
k=len(final_csv.head(2))
recg,unrecg=0,0
if k==2:
	unrecg,recg = final_csv.filter(final_csv.recognized==False).collect()[0]["average"],final_csv.filter(final_csv.recognized==True).collect()[0]["average"]
	print(f"{recg:.5f}")
	print(f"{unrecg:.5f}")
elif k==0:
	print(f"{recg:.5f}")
	print(f"{unrecg:.5f}")
else:
	if (final_csv.filter(final_csv.recognized==False).head(1)):
		unrecg=final_csv.filter(final_csv.recognized==False).collect()[0]["average"]
	else:
		recg=final_csv.filter(final_csv.recognized==True).collect()[0]["average"]
	print(f"{recg:.5f}")
	print(f"{unrecg:.5f}")
'''
true_csv=final_csv.filter(final_csv.recognized==True)
false_csv=final_csv.filter(final_csv.recognized=False)
true_sum=true_csv.sum(
'''


