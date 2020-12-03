  
#!/usr/bin/python3
import os
import shutil
#deleting all files except recent
l=["/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/matchdata","/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/chem",\
    "/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerdata","/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerrank",\
        "/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerreg"]

for folder in l:
    folders=[os.path.join(folder,d) for d in os.listdir(folder)]
    for file_names in sorted(folders,key=os.path.getmtime)[:-1]:
        shutil.rmtree(file_names)
    for file_name in [os.path.join(folder,d) for d in os.listdir(folder)]:
        os.rename(file_name,file_name.split("-")[0])

#main UI
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









#!/usr/bin/python3
import os
import shutil
#deleting all files except recent
l=["/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/matchdata","/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/chem",\
    "/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerdata","/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerrank",\
        "/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/data/playerreg"]
#l=["/home/sreyans/Desktop"]
for folder in l:
    folders=[os.path.join(folder,d) for d in os.listdir(folder)]
    for file_names in sorted(folders,key=os.path.getmtime)[:-1]:
        shutil.rmtree(file_names)
    for file_name in [os.path.join(folder,d) for d in os.listdir(folder)]:
        os.rename(file_name,file_name.split("-")[0])

#main UI
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
