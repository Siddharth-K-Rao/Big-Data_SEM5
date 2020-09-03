#!usr/bin/python3
import sys
current_city=""
count=0
for i in sys.stdin:
	line=i.split("\t")
	if line[0]!=current_city:
		if count:
			print(current_city,count)
		current_city=line[0]
		count=1
	else:
		count+=1
if current_city:
	print(current_city,count)
