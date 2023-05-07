# ingest data from HPC to HDFS
hdfs dfs -put final_project/data/Chi_Crimes_2001_to_Present.csv final_project/data
# take processed data back from HDFS to HPC
hdfs dfs -get final_project