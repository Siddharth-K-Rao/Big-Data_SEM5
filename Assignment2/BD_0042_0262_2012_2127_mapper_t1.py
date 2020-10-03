#!/usr/bin/python3
import sys

#Reading the input as it is
for inputline in sys.stdin:
    line=inputline.strip()
    if line[0]=="#":
        continue
    try:
        from_node,to_node=line.split("\t")
        print(from_node,to_node,sep=" ")
    except:
        #print("here")
        continue
