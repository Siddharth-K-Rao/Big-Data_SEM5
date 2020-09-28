#!/usr/bin/python3
import sys

for inputline in sys.stdin:
    line=inputline.strip()
    if line[0]=="#":
        continue
    try:
        from_node,to_node=line.split("\t")
        print(from_node,to_node,sep=" ")
    except:
        print("here")
        break