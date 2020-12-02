#!/usr/bin/python3
#python script to delete files in linux file_system
l=["/home/revanth/Desktop/FPL/matchdata/matchinfo","/home/revanth/Desktop/FPL/chem/chemdata",\
    "/home/revanth/Desktop/FPL/playerdata/playerinfo","/home/revanth/Desktop/FPL/playerrank/rating",\
        "/home/revanth/Desktop/FPL/playerreg/players"]
#l=["/home/sreyans/Desktop"]

import os
import shutil
for folder in l:
    folders=[os.path.join(folder,d) for d in os.listdir(folder)]
    for file_names in sorted(folders,key=os.path.getmtime)[:-1]:
        shutil.rmtree(file_name)
    for file_name in [os.path.join(folder,d) for d in os.listdir(folder)]:
        os.rename(file_name,file_name.split("-")[0])

'''
k=r"Niki M\u00e4enp\u00e4\u00e4"
z=k.encode('utf-8')
x="Niki M\u00e4enp\u00e4\u00e4"
print(z.decode()==x)
print(z)
#print(k,str(z)[1:])
'''
'''
import json
inpu=open("inp.json","r")
x=inpu.read()
#print(x)
x=x.replace("\\u","\\\\u")
y=json.loads(x)
for i in y['team1']:
    print(y['team1'][i])
        #print(j)
        #print(y['team1'][i][j])
# for i in y['team1']:
#     if i!='name':
#         x=""
#         for j in y['team1'][i]:
#             print(j,end=";")
#             #try:
#             #    if j=="\":
#             #        x=x+"\\"
#             #except:
#             #    print(j)
#             #else:
#             #    x=x+j
#         y['team1'][i]=x
# print()
print(y)
'''