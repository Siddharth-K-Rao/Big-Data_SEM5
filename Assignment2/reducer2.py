#!/usr/bin/python3
import sys
initnode=""
initsum=0
v1_file = open("v1","w")
for eachline in sys.stdin:
    line=eachline.strip()
    node,contrib = line.split(" ")
    if initnode!=node:
        if initnode!="":
            val=round(0.15+0.85*initsum,4)
            v1_file.write(f"{initnode}, {val}\n")
        initnode = node
        initsum = float(contrib)
    else:
        initsum+= float(contrib)
if initnode:
    val=round(0.15+0.85*initsum,4)
    v1_file.write(f"{initnode}, {val}\n")
v1_file.close()

