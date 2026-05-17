#!/bin/bash
# ============================================================
#  run_pig_job.sh — E-Commerce Sales Analysis (Apache Pig)
# ============================================================
set -e

echo "======================================"
echo " E-Commerce Sales Analysis — Apache Pig"
echo "======================================"

# 1. Start services
su - hadoop -c "start-dfs.sh && start-yarn.sh"
su - hadoop -c "mapred --daemon start historyserver"
sleep 5
jps

# 2. Upload dataset to HDFS
hdfs dfs -mkdir -p /assignment/pig
hdfs dfs -put data/sales.csv /assignment/pig/
hdfs dfs -ls /assignment/pig

# 3. Run Pig script
pig src/analysis.pig

# 4. Verify outputs
echo "--- Best Selling Products ---"
hdfs dfs -cat /assignment/pig/A7/O1/best_selling_products/part-r-00000

echo "--- Category Revenue ---"
hdfs dfs -cat /assignment/pig/A7/O2/category_revenue/part-r-00000

echo "--- Peak Buying Hours ---"
hdfs dfs -cat /assignment/pig/A7/O3/peak_buying_hours/part-r-00000

echo "======================================"
echo " Pig Job Completed Successfully!"
echo "======================================"
