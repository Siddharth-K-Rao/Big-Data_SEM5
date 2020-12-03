#!/usr/bin/python3
import os
import shutil
l=["/home/revanth/Desktop/FPL/matchdata/matchinfo","/home/revanth/Desktop/FPL/chem/chemdata",\
    "/home/revanth/Desktop/FPL/playerdata/playerinfo","/home/revanth/Desktop/FPL/playerrank/rating",\
        "/home/revanth/Desktop/FPL/playerreg/players"]
#l=["/home/sreyans/Desktop"]
for folder in l:
    folders=[os.path.join(folder,d) for d in os.listdir(folder)]
    for file_names in sorted(folders,key=os.path.getmtime)[:-1]:
        shutil.rmtree(file_name)
    for file_name in [os.path.join(folder,d) for d in os.listdir(folder)]:
        os.rename(file_name,file_name.split("-")[0])
print("\n################################## FPL_ANALYTICS ##################################\n\n")

while(True):
    try:
        arg1=input("#Enter input file location: ")
        arg1 = arg1.replace("\\","/")
        arg2=input("#Enter output file location: ")
        arg2 = arg2.replace("\\","/")
        os.chdir("/opt/spark")
        s='''spark-submit '/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/metrics.py' '''+arg1+" "+arg2
        #print(s)
        print("\nRunning...............................................................................")
        os.system(s)
        print("Output\n")
        os.system("cat "+arg2)
        print("\nDONE\n\n")
    except:
        break
print("\n###################################################################################")