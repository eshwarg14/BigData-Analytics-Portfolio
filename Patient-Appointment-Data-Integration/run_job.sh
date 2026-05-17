#!/bin/bash
# run_job.sh — Patient Appointment Data Integration (Reduce-Side Join)
set -e
su - hadoop -c "start-dfs.sh && start-yarn.sh"
hdfs dfs -mkdir -p /assignment/A4RC
hdfs dfs -put data/patients.txt     /assignment/A4RC/
hdfs dfs -put data/appointments.csv /assignment/A4RC/
hadoop jar ~/Downloads/Hadoop_Jar/PD.jar PatientAppointmentJoin \
  /assignment/A4RC/patients.txt \
  /assignment/A4RC/appointments.csv \
  /assignment/A4RC/output
hdfs dfs -cat /assignment/A4RC/output/part-r-00000
su - hadoop -c "stop-all.sh"
