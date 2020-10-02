#!/bin/sh
CONVERGE=1
rm v* log*

$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave
hdfs dfs -rm -r /output* 

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-*streaming*.jar \
-mapper "python3 /home/sreyans/Downloads/BD_0042_0262_2012_2127_mapper_t1.py" \
-reducer "python3 /home/sreyans/Downloads/BD_0042_0262_2012_2127_reducer_t1.py '/home/sreyans/Downloads/v'"  \
-input /dataset-A2 \
-output /output1 #has adjacency list

echo "DONE"
while [ "$CONVERGE" -ne 0 ]
do
	$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-*streaming*.jar \
	-mapper "python3 /home/sreyans/Downloads/BD_0042_0262_2012_2127_mapper_t2.py '/home/sreyans/Downloads/v'" \
	-reducer "python3 /home/sreyans/Downloads/BD_0042_0262_2012_2127_reducer_t2.py" \
	-input /output1 \
	-output /output2
	touch v1
	hadoop fs -cat /output2/* > v1
	CONVERGE=$(python3 check_conv.py >&1)
	hdfs dfs -rm -r /output2
	echo $CONVERGE
done
