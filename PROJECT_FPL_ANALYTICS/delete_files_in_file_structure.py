#python script to delete files in linux file_system
l=["/home/revanth/Desktop/FPL/matchdata/matchinfo","/home/revanth/Desktop/FPL/chem/chemdata",\
    "/home/revanth/Desktop/FPL/playerdata/playerinfo","/home/revanth/Desktop/FPL/playerrank/rating",\
        "/home/revanth/Desktop/FPL/playerreg/players"]
#l=["/home/sreyans/Desktop"]
import os
for folder in l:
    folders=[os.path.join(folder,d) for d in os.listdir(folder)]
    for file_name in sorted(folders,key=os.path.getmtime)[:-1]:
        print(file_name)
        #os.remove(file_name)
