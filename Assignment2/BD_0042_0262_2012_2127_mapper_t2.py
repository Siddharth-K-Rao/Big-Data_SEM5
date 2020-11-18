#!/usr/bin/python3

import sys
location=sys.argv[1]

#getting the page rank of all nodes -> gave empty node_list many times when loop
#was not there
node_list={}
#while(not(node_list)):CHANGE
v_file=open(location,"r")
for line in v_file:
    from_node,page_rank=line.split(",")
    page_rank=page_rank.strip()
    node_list[from_node]=float(page_rank)
v_file.close()

#reading from adjacenecy list
for eachline in sys.stdin:
    line=eachline.strip()
    from_node,to_nodes = line.split("\t")
    to_nodes=to_nodes.strip("[] ")
    to_nodes = [i.strip() for i in to_nodes.split(",")]
    n=len(to_nodes)
    val=float((1/n)*node_list[from_node])
    for i in to_nodes:
        if i in node_list:
            print(i,val,sep=" ")
    print(from_node,0,sep=" ")#getting all the parent nodes also
    #print(to_nodes)
