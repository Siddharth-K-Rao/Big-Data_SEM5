#!usr/bin/python3
import sys
for lines in sys.stdin:
	line=lines.split(";")
	if not(line) or len(line)<9:
		continue
	if line[8]=="gas":
		print(line[1],"\t",1)
	
