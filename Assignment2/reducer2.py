#!/usr/bin/python3
import sys
initnode=""
initsum=0
v1_file = open("v1","w")
for eachline in sys.stdin:
    #print(eachline)
    line=eachline.strip()
    node,contrib = line.split(" ")
    if initnode!=node:
        if initnode!="":
            val=round(0.15+0.85*initsum,5)
            k=str(val)
            if len(k[k.index("."):])<6:
                k=k+"0"*(6-len(k[k.index("."):]))
            v1_file.write(f"{initnode}, {k}\n")
        initnode = node
        initsum = float(contrib)
    else:
        initsum+= float(contrib)
if initnode:
    val=round(0.15+0.85*initsum,5)
    k=str(val)
    if len(k[k.index("."):])<6:
                k=k+"0"*(6-len(k[k.index("."):]))
    v1_file.write(f"{initnode}, {k}\n")
v1_file.close()

