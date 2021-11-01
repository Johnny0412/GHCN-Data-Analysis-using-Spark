# DATA420-21S2 Assignment 1
# Name：Xin Gao (43044879)
#####################################################################################
# Processing Q1
#####################################################################################

# Determine how the data is structured in /data/ghcnd/

hdfs dfs -ls /data/ghcnd/
hdfs dfs -ls /data/ghcnd/daily | head
hdfs dfs -ls /data/ghcnd/daily | tail


# Determine how many files there are in daily

hdfs dfs -ls /data/ghcnd/daily | wc -l

# Determine the size of all of the data and the size of daily specifically

hdfs dfs -du -h /data
hdfs dfs -du -h /data/ghcnd
hdfs dfs -du -h /data/ghcnd/daily


#####################################################################################
# Processing Q2
#####################################################################################
# Peek at the top of each data file

hdfs dfs -cat /data/ghcnd/ghcnd-countries.txt | head
hdfs dfs -cat /data/ghcnd/ghcnd-inventory.txt | head
hdfs dfs -cat /data/ghcnd/ghcnd-states.txt | head
hdfs dfs -cat /data/ghcnd/ghcnd-stations.txt | head

# Determine how many countries without using pyspark

hdfs dfs -cat /data/ghcnd/ghcnd-countries.txt | wc -l

# Peek at Gunzip daily

hdfs dfs -cat /data/ghcnd/daily/2021.csv.gz | gunzip | head


#####################################################################################
# Analysis Q3
#####################################################################################
# Determine the size of daily 2021 and 2015
hdfs dfs -du -h /data/ghcnd/daily/2021.csv.gz
hdfs dfs -du -h /data/ghcnd/daily/2015.csv.gz

# Determine he individual block sizes for the year 2015
hdfs fsck hdfs:///data/ghcnd/daily/2015.csv.gz -files -blocks


#####################################################################################
# Analysis Q4
#####################################################################################
# Copy the output from HDFS to local home directory
hdfs dfs -copyToLocal hdfs:///user/xga37/outputs/ghcnd/dailyTNZ /users/home/xga37/dailyTNZ
wc -l /users/home/xga37/dailyTNZ

# Count the number of rows in the part files using the wc -l bash command
wc -l /users/home/xga37/dailyTNZ


