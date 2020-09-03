#!/usr/bin/python3

import sys
import json
import math

current_country = ""
count = 0

for line in sys.stdin:
    country,c = line.strip().split('\t')
    try:
        c = int(c)
    except:
    	continue
    if(country != current_country):
        if current_country != "":
            print(current_country,count,sep=',')
        current_country = country
        count = 1
    else:
        count += 1
if current_country:
    print(current_country,count,sep = ',')
