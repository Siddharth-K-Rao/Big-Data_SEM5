#!/usr/bin/python3
import sys
current_word=""
noc=0
for i in sys.stdin:
	word,count=i.strip().split("\t")
	try:
		count=int(count)
	except:
		print("Count is not a number")
		continue
	if word!=current_word:
		if current_word!="":
			print(current_word,noc)
		current_word=word
		noc=1
	else:
		noc+=1
if current_word:
	print(current_word,noc)

			
	
