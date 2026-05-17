#!/bin/bash
# ============================================================
#  run_streaming_job.sh — Web Server Log Analytics
#  Hadoop Streaming with Python mapper + reducer
# ============================================================
set -e

STREAMING_JAR="/home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar"
PYTHON_BIN="/home/eshwar/Documents/env312/bin/python3"
MAPPER_PATH="/home/eshwar/Downloads/Python/Log/Mapper.py"
REDUCER_PATH="/home/eshwar/Downloads/Python/Log/Reducer.py"
INPUT_HDFS="/assignment/Streaming/access.log"
OUTPUT_HDFS="/assignment/Streaming/out"

echo "======================================"
echo " Web Server Log Analytics — Streaming"
echo "======================================"

# 1. Start Hadoop
su - hadoop -c "start-dfs.sh && start-yarn.sh"
sleep 5

# 2. Upload access log
hdfs dfs -mkdir -p /assignment/Streaming
hdfs dfs -put data/access.log $INPUT_HDFS
echo "Input file uploaded:"
hdfs dfs -ls /assignment/Streaming

# 3. Copy Python scripts to accessible path
mkdir -p ~/Downloads/Python/Log
cp src/mapper.py  $MAPPER_PATH
cp src/reducer.py $REDUCER_PATH
chmod +x $MAPPER_PATH $REDUCER_PATH

# 4. Test locally first
echo "--- Local Test ---"
cat data/access.log | python3 src/mapper.py | sort | python3 src/reducer.py

# 5. Run Hadoop Streaming job
echo "--- Running Hadoop Streaming Job ---"
hadoop jar $STREAMING_JAR \
  -input   $INPUT_HDFS \
  -output  $OUTPUT_HDFS \
  -mapper  "$PYTHON_BIN $MAPPER_PATH" \
  -reducer "$PYTHON_BIN $REDUCER_PATH"

# 6. Display output
echo "--- Output ---"
hdfs dfs -cat $OUTPUT_HDFS/part-00000

echo "======================================"
echo " Streaming Job Completed!"
echo "======================================"
