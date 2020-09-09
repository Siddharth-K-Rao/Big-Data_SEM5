#!/usr/bin/python3
import sys
import json
import datetime
true=True
false=False
citysearch=sys.argv[1]
# print(citysearch)
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
    and ("timestamp" in line) and len(line)==6:
            return True
    return False


for lines in sys.stdin:
    try:
        line=json.loads(lines)
    except NameError:
        continue
    if checkall(line):
        if line['word']==citysearch:
            if line['recognized']:
                print("0\t1")
            else:
                date1=line['timestamp'].split(" ")
                date1=date1[0]
                date1=datetime.datetime.strptime(date1,"%Y-%m-%d").weekday()
                if date1==5 or date1==6:
                    print("1\t1")
                # else:
                #     print("nonn")
    else:
        # print("yoyo")
        continue

    # print("done",sys.argv[1])
    # print(line)

