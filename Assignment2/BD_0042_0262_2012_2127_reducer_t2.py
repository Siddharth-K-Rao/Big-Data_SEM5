#!/usr/bin/python3
import sys
initnode=""
initsum=0
for eachline in sys.stdin:
    line=eachline.strip()
    node,contrib = line.split(" ")
    if initnode!=node:
        if initnode!="":
            val=0.15+0.85*initsum
            print(f"{initnode}, {val:.5f}")
        initnode = node
        initsum = float(contrib)
    else:
        initsum+= float(contrib)
if initnode:
    val=0.15+0.85*initsum
    print(f"{initnode}, {val:.5f}")


