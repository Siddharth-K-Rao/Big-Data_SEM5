#!/usr/bin/python3
f=open("shape_stat.csv","r")
count=0
word="alarm clock"
for i in f.readlines():
	k=i.split(",")
	
	if k[0]<word or k[0]=='word' or k[0]=='sreyans':
		continue
	if k[0]==word:
		if k[2]=='False' and int(k[4])<10:
			count+=1
	else:
		print(k[0])
		break
print(count)
