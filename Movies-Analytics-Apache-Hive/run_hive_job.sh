#!/bin/bash
# ============================================================
#  run_hive_job.sh — Movies & Directors Analytics (Apache Hive)
# ============================================================
set -e

echo "======================================"
echo " Movies Analytics — Apache Hive"
echo "======================================"

# 1. Start Hadoop services
su - hadoop -c "start-dfs.sh && start-yarn.sh"
sleep 5

# 2. Start HiveServer2 (if not already running)
# hiveserver2 &
# sleep 10

# 3. Copy datasets to local Hive-accessible path
mkdir -p ~/Downloads/A8
cp data/movies.csv   ~/Downloads/A8/
cp data/directors.csv ~/Downloads/A8/

# 4. Run HiveQL script via Beeline
echo "Running HiveQL script..."
beeline -u jdbc:hive2:// -f src/movies_analytics.hql

# 5. Verify HDFS partition structure
echo "--- HDFS Partition Structure ---"
hdfs dfs -ls /user/hive/warehouse/movies.db/movies

echo "======================================"
echo " Hive Job Completed Successfully!"
echo "======================================"
