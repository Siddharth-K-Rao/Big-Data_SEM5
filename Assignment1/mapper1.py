#!/usr/bin/python3

import sys
import json
import math

s = sys.argv[1]
k = int(sys.argv[2])
            
            
def checkword(s):
    for i in s:
        if not(i.isalpha() or i==" "):
            return False
    return True if s!="" else False

def checkcountry(s):
    if len(s)==2 and s[0]>='A' and s[0]<='Z' and s[1]>='A' and s[1]<='Z':
        return True
    return False

def checkkey(s):
    if len(s)==16 and s.isdigit():
        return True
    return False

def checkdraw(s):
    if not(s):
        return False
    for i in s:
        if not(len(i)==2 and len(i[0])==len(i[1])):
            return False
    return True

def checkall(line):
    if ('word' in line and checkword(line['word'])) and ('countrycode' in line and checkcountry(line['countrycode'])) and (('recognized' in line)\
    and type(line['recognized'])==bool) and ('key_id' in line and checkkey(line['key_id'])) and ('drawing' in line and checkdraw(line['drawing'])) \
    and ("timestamp" in line):
            return True
    return False


for line in sys.stdin:
    line = line.strip()
    d = json.loads(line)
    if checkall(d):
        if (d['word'] == s):
            dist = math.sqrt((d['drawing'][0][0][0] ** 2) + (d['drawing'][0][1][0] ** 2))
            if dist > k:
                print('%s\t%s'%(d['countrycode'],1))
