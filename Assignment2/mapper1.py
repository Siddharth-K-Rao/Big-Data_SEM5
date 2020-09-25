#!/usr/bin/python3
import sys

for inputline in sys.stdin:
    line=inputline.strip()
    if line[0]=="#":
        continue
    from_node,to_node=line.split(" ")
    print(from_node,to_node,sep=" ")