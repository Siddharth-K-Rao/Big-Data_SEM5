#!/usr/bin/python3
import sys
cur_node=""
cur_list=[]
v_file = open("v","w")
for inputline in sys.stdin:
    line=inputline.strip()
    from_node,to_node=line.split(" ")
    if from_node!=cur_node:
        if cur_node!="":
            #print(cur_node,cur_list)
            #cur_list.remove(int(cur_node))
            print(cur_node,sorted(cur_list),sep="\t")
            cur_list=[]
        cur_node=from_node
        v_file.write(f"{cur_node}, 1\n")
    try:
        cur_list.append(int(to_node))
    except:
        cur_list.append(int("".join(to_node.split(","))))
if cur_node:
    #print(cur_node,cur_list)
    #cur_list.remove(int(cur_node))
    print(cur_node,sorted(cur_list),sep="\t")
v_file.close()

        
    





