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
