#!/bin/bash
# run_job.sh — Product Sentiment Partition Analysis
set -e
su - hadoop -c "start-dfs.sh && start-yarn.sh"
hdfs dfs -mkdir -p /assignment/wordcount
hdfs dfs -put data/reviews.txt /assignment/wordcount/
hadoop jar ~/Downloads/Hadoop_Jar/reviewpart.jar ReviewPartitioner \
  /assignment/wordcount/reviews.txt \
  /assignment/wordcount/outputpart
echo "--- Negative (damaged) ---"
hdfs dfs -cat /assignment/wordcount/outputpart/part-r-00000
echo "--- Positive (good) ---"
hdfs dfs -cat /assignment/wordcount/outputpart/part-r-00001
su - hadoop -c "stop-all.sh"
