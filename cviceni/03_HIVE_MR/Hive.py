#!/usr/bin/env python
# coding: utf-8

# 1. copy data from HDFS folder `/user/pascepet/data/teplota-usa.zip` to your HDFS-home folder under some new directory `/user/username/some_new_dir`
# 2. download data from HDFS to your home folder and unzip it. (hint `unzip archive.zip`)
# 3. upload csv files to HDFS
# 
# On hador local system:
# ```bash
# hadoop fs -get /user/pascepet/data/teplota-usa.zip
# unzip teplota-usa.zip
# hadoop fs -mkdir data
# hadoop fs -mkdir data/03
# hadoop fs -put *.csv data/03
# ```

# start HIVE CLI using command
# 
# `beeline -u "jdbc:hive2://hador-c1.ics.muni.cz:10000/default;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ"`

# On Hive prompt:
# 
# 1 Create external table
# 
# - Create your database (if not exists)
# ```sql
# show databases;
# create database auratons;
# ```
# - Make your database your working database
# ```sql
# use auratons;
# ```
# - Create external table name temperature_tmp, csv file is separated by ","
# 
# | Column name | Data type |
# |:------------|:----------|
# | stanice     | string    |
# | mesic       | int       |
# | den         | int       |
# | hodina      | int       |
# | teplota     | double    |
# | flag        | string    |
# | latitude    | double    |
# | longitude   | double    |
# | vyska       | double    |
# | stat        | string    |
# | nazev       | string    |
# 
# ```sql
# CREATE EXTERNAL TABLE IF NOT EXISTS ap_temp (
#     stanice STRING,
#     mesic INT,
#     den INT,
#     hodina INT,
#     teplota DOUBLE,
#     flag STRING,
#     latitude DOUBLE,
#     longitude DOUBLE,
#     vyska DOUBLE,
#     stat STRING,
#     nazev STRING
# )
# ROW FORMAT
# DELIMITED FIELDS TERMINATED BY ','
# LINES TERMINATED BY '\n'
# STORED AS TEXTFILE
# LOCATION '/user/auratons/data/03';
# ```

# 2 Create internal table
# 
# - Create internal table named temperature stored as parquet with snappy compression codec
# ```sql
# CREATE TABLE IF NOT EXISTS temperature (
#     stanice STRING,
#     mesic INT,
#     den INT,
#     hodina INT,
#     teplota DOUBLE,
#     flag STRING,
#     latitude DOUBLE,
#     longitude DOUBLE,
#     vyska DOUBLE,
#     stat STRING,
#     nazev STRING
# )
# STORED AS PARQUET tblproperties
# ("parquet.compress"="SNAPPY");
# ```
# - Insert data into internal table. Convert temperature data from 10xFahrenheit to celsius using formula $ (\frac{F}{10} - 32) \times \frac{5}{9} $
# 
# **TODO Do the conversion**
# 
# ```sql
# INSERT OVERWRITE TABLE temperature
# SELECT
#     stanice,
#     mesic,
#     den,
#     hodina,
#     teplota,
#     flag,
#     latitude,
#     longitude,
#     vyska,
#     stat,
#     nazev
# FROM ap_temp;
# ```
# - Drop external table
# ```sql
# DROP TABLE ap_temp;
# ```
# - Check that data files are still on HDFS (`hdfs:///user/username/teplota/`)

# 3 Find a state with the highest average temperature in summer (month 6, 7, 8)
# 
# 
# | State | AVG_TEMP |
# |:------|:---------|
# |       |          |
# 

# 4 Create internal partitioned table
# 
# - Create table partitioned by month use snappy compression
# - Insert data into partitioned table
# - Inspect partitioned folder on HDFS (`/user/hive/warehouse/username.db/`)
# 
# To enable dynamic partitioning execute this commands
# 
# ```
# set hive.exec.dynamic.partition=true;
# set hive.exec.dynamic.partition.mode=nonstrict;
# ```
# 

# 5 Advanced SQL
# 
# I. Find states with the highest average temperature per month 
# 
# | Month | State | AVG_TEMP |
# |:------|:------|:---------|
# |       |       |          |
# 
# 
# II. Find weekly seasonality for each station
# 
# | station | avg_temp_monday | ... | avg_temp_sunday |
# |:------|:------|:---------|:---------|
# |       |       |          ||
# 
# 
# III. Find the difference between station temperature and state's average temperature
# 
# | station | diff |
# |:------|:------|
# |       |       |
# 
# 
# 
# (hint [Hive Windowing Functions](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+WindowingAndAnalytics))
