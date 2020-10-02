#!/usr/bin/python3
import sys
initnode=""
initsum=0
for eachline in sys.stdin:
    #print(eachline)
    line=eachline.strip()
    node,contrib = line.split(" ")
    if initnode!=node:
        if initnode!="":
            val=0.15+0.85*initsum
            # k=str(val)
            # if len(k[k.index("."):])<6:
            #     k=k+"0"*(6-len(k[k.index("."):]))
            print(f"{initnode}, {val:.5f}")
        initnode = node
        initsum = float(contrib)
    else:
        initsum+= float(contrib)
if initnode:
    val=0.15+0.85*initsum
    # k=str(val)
    # if len(k[k.index("."):])<6:
    #             k=k+"0"*(6-len(k[k.index("."):]))
    print(f"{initnode}, {val:.5f}")

