#!/bin/bash
# run_job.sh — Student Performance Distributed Cache Analysis
set -e
su - hadoop -c "start-dfs.sh && start-yarn.sh"
hdfs dfs -mkdir -p /practice/DClass
hdfs dfs -put data/s.csv /practice/DClass/
hdfs dfs -put data/m.csv /practice/DClass/
hadoop jar ~/Downloads/Hadoop_Jar/DSM.jar dist \
  /practice/DClass/m.csv \
  /practice/DClass/output1
hdfs dfs -cat /practice/DClass/output1/part-r-00000
su - hadoop -c "stop-all.sh"
