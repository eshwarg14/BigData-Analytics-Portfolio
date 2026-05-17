#!/bin/bash
# ============================================================
#  run_job.sh — Damaged Product Review Analysis (MapReduce)
#  Usage: bash run_job.sh
# ============================================================

set -e

JAR_PATH="$HOME/Downloads/Hadoop_Jar/review.jar"
INPUT_HDFS="/assignment/wordcount"
OUTPUT_HDFS="/assignment/wordcount/output"
LOCAL_INPUT="$HOME/Downloads/reviews.txt"

echo "======================================"
echo " Damaged Product Review Analysis Job"
echo "======================================"

# 1. Start Hadoop services
echo "[1/6] Starting Hadoop DFS and YARN..."
su - hadoop -c "start-dfs.sh"
su - hadoop -c "start-yarn.sh"

# 2. Wait for services to initialise
sleep 5
echo "[2/6] Checking Java processes..."
jps

# 3. Create HDFS input directory
echo "[3/6] Creating HDFS directories..."
hdfs dfs -mkdir -p $INPUT_HDFS

# 4. Upload input file to HDFS
echo "[4/6] Uploading reviews.txt to HDFS..."
cp data/reviews.txt $LOCAL_INPUT
hdfs dfs -put $LOCAL_INPUT $INPUT_HDFS/
hdfs dfs -ls $INPUT_HDFS

# 5. Run the MapReduce job
echo "[5/6] Running MapReduce job..."
hadoop jar $JAR_PATH wordcount $INPUT_HDFS $OUTPUT_HDFS

# 6. Verify and display output
echo "[6/6] Displaying output..."
hdfs dfs -ls $OUTPUT_HDFS
hdfs dfs -cat $OUTPUT_HDFS/part-r-00000

# 7. Save output to local filesystem
echo "Saving output locally..."
hdfs dfs -get $OUTPUT_HDFS/part-r-00000 ./output/part-r-00000

# 8. Stop Hadoop
echo "Stopping Hadoop..."
su - hadoop -c "stop-all.sh"

echo "======================================"
echo " Job Completed Successfully!"
echo "======================================"
