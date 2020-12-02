#!/usr/bin/python3
import os
print("################################## FPL_ANALYTICS ##################################")
while(True):
    try:
        arg1=input("#Enter input file location: ")
        arg1.replace("\\","/")
        arg2=input("#Enter output file location: ")
        arg2.replace("\\","/")
        os.chdir("/opt/spark")
        s='''spark-submit '/home/sreyans/Desktop/SEM5/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/metrics.py' '''+arg1+" "+arg2
        #print(s)
        os.system(s)
    except:
        break
print("\n###################################################################################")