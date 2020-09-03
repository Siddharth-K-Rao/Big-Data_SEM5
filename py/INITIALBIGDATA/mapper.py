#!/usr/bin/python3
import sys
for line in sys.stdin:
	lines=line.strip()
	lines=lines.split()
	for i in lines:
		print(f"{i}\t1")
	
