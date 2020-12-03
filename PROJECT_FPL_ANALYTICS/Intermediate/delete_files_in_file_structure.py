#!/usr/bin/python3
#python script to delete files in linux file_system
l=["/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/matchdata","/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/chem",\
    "/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerdata","/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerrank",\
        "/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerreg"]
#l=["/home/sreyans/Desktop"]
import os
import shutil
for folder in l:
    folders=[os.path.join(folder,d) for d in os.listdir(folder)]
    for file_names in sorted(folders,key=os.path.getmtime)[:-1]:
        shutil.rmtree(file_names)
    for file_name in [os.path.join(folder,d) for d in os.listdir(folder)]:
        os.rename(file_name,file_name.split("-")[0])
