# ingest data from HPC to HDFS
hdfs dfs -put nypd_raw_data.csv final_project/data
# take processed data back from HDFS to HPC
hdfs dfs -get final_project