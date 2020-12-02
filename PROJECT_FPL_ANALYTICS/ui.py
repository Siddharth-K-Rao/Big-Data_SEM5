#!/usr/bin/python3
import os
print("\n################################## FPL_ANALYTICS ##################################\n\n")

while(True):
    try:
        arg1=input("#Enter input file location: ")
        arg1 = arg1.replace("\\","/")
        arg2=input("#Enter output file location: ")
        arg2 = arg2.replace("\\","/")
        os.chdir("/opt/spark")
        s='''spark-submit '/home/revanth/Desktop/SEM5/BD/Big_Data_SEM5/PROJECT_FPL_ANALYTICS/test.py' '''+arg1+" "+arg2
        #print(s)
        print("\nRunning...............................................................................")
        os.system(s)
        print("Output\n")
        os.system("cat "+arg2)
        print("\nDONE\n\n")
    except:
        break
print("\n###################################################################################")