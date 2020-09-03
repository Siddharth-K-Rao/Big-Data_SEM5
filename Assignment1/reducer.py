#!usr/bin/python3
import sys
rec=0
nrec=0
for lines in sys.stdin:
    line=lines.strip()
    word,count=line.split("\t")
    try:
        count=int(count)
    except:
        continue
    if word=='0':
        rec+=count
    else:
        nrec+=count
print(rec)
print(nrec)

