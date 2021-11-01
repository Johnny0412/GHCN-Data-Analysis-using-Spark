# DATA420-21S2 Assignment 1
# Name：Xin Gao (43044879)
#####################################################################################
# Processing Q2 (a)
#####################################################################################

import sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M 

#Check the conf information
#4 executors, 2 core per executor, 4 GB of executor memory, and 4 GB of master memory
sc.getConf().getAll()

# Schema of daily
schema_daily = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", DateType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", IntegerType(), True),
    StructField("MEASUREMENT_FLAG", StringType(), True),
    StructField("QUALITY_FLAG", StringType(), True),
    StructField("SOURCE_FLAG", StringType(), True),
    StructField("OBSERVATION_TIME", StringType(), True)
])

# Schema of stations
schema_stations = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", StringType(), True),
    StructField("LONGITUDE", StringType(), True),
    StructField("ELEVATION", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("GSN_FLAG", StringType(), True),
    StructField("HCN/CRN_FLAG", StringType(), True),
    StructField("WMO_ID", StringType(), True)
])

# Schema of countries
schema_countries = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True)
])

# Schema of states
schema_states = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True)
])

# Schema of inventory
schema_inventory = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", StringType(), True),
    StructField("LONGITUDE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("FIRSTYEAR", StringType(), True),
    StructField("LASTYEAR", StringType(), True),
])

#####################################################################################
# Processing Q2 (b)
#####################################################################################
# Load 1000 rows of daily 2021
daily2021 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("dateFormat", "yyyyMMdd")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2021.csv.gz")
    .limit(1000)
)
daily2021.cache()
daily2021.show(10, False)

#####################################################################################
# Processing Q2 (c)
#####################################################################################
# Parse the fixed width text formatting
# Load stations
stations = spark.read.text("hdfs:///data/ghcnd/ghcnd-stations.txt")
stations = stations.select(
    F.trim(stations.value.substr(1,11)).alias("ID").cast(StringType()),
    F.trim(stations.value.substr(13,8)).alias("LATITUDE").cast(StringType()),
    F.trim(stations.value.substr(22,9)).alias("LONGITUDE").cast(StringType()),
    F.trim(stations.value.substr(32,6)).alias("ELEVATION").cast(StringType()),
    F.trim(stations.value.substr(39,2)).alias("STATE").cast(StringType()),
    F.trim(stations.value.substr(42,30)).alias("NAME").cast(StringType()),
    F.trim(stations.value.substr(73,3)).alias("GSN_FLAG").cast(StringType()),
    F.trim(stations.value.substr(77,3)).alias("HCN/CRN_FLAG").cast(StringType()),
    F.trim(stations.value.substr(81,5)).alias("WMO_ID").cast(StringType())
)
stations.show(10, False)
 
# Load countries
countries = spark.read.text("hdfs:///data/ghcnd/ghcnd-countries.txt")
countries = countries.select(
    F.trim(countries.value.substr(1,2)).alias("CODE").cast(StringType()),
    F.trim(countries.value.substr(4,47)).alias("NAME").cast(StringType())
    )
countries.cache()
countries.show(10, False)

# Load states
states = spark.read.text("hdfs:///data/ghcnd/ghcnd-states.txt")
states = states.select(
    F.trim(states.value.substr(1,2)).alias("CODE").cast(StringType()),
    F.trim(states.value.substr(4,47)).alias("NAME").cast(StringType())
)
states.cache()
states.show(10, False)

# Load inventory
inventory = spark.read.text("hdfs:///data/ghcnd/ghcnd-inventory.txt")
inventory = inventory.select(
    F.trim(inventory.value.substr(1,11)).alias("ID").cast(StringType()),
    F.trim(inventory.value.substr(13,8)).alias("LATITUDE").cast(StringType()),
    F.trim(inventory.value.substr(22,9)).alias("LONGITUDE").cast(StringType()),
    F.trim(inventory.value.substr(32,4)).alias("ELEMENT").cast(StringType()),
    F.trim(inventory.value.substr(37,4)).alias("FIRSTYEAR").cast(StringType()),
    F.trim(inventory.value.substr(42,4)).alias("LASTYEAR").cast(StringType())
)
inventory.cache()
inventory.show(10, False)

#How many rows are in each metadata table?
print(stations.count())
print(countries.count())
print(states.count())
print(inventory.count())

#How many stations do not have a WMO ID?
print(stations.filter(F.col("WMO_ID") == "").count())

#####################################################################################
# Processing Q3 (a)
#####################################################################################
#Extract the two character country code from each station code
stations = (
    stations.withColumn("COUNTRY_CODE", F.substring(F.col("ID"), 1, 2))
    )
stations.show(10, False)

#####################################################################################
# Processing Q3 (b)
#####################################################################################
#LEFT JOIN stations with countries
stations = (
    stations
    .join(countries.withColumnRenamed("NAME", "COUNTRY_NAME"),
          F.col("COUNTRY_CODE") == F.col("CODE"),
          how = "left"
          )
    .drop("CODE")
    )
stations.show(10, False)

#####################################################################################
# Processing Q3 (c)
#####################################################################################
#LEFT JOIN stations and states
stations = (
    stations
    .join(states.withColumnRenamed("NAME", "STATE_NAME"),
                         F.col("STATE") == F.col("CODE"),
                         how = "left")
    .drop("CODE")
    )
stations.show(10, False)

#####################################################################################
# Processing Q3 (d)
#####################################################################################
#First and last year that each station was active
stationsActive = (
    inventory
    .groupBy("ID")
    .agg(F.min(F.col("FIRSTYEAR")).alias("START_YEAR"),
         F.max(F.col("LASTYEAR")).alias("END_YEAR")
         )
    )
stationsActive.show(10, False)

#How many different elements has each station collected overall?
stationElements = (
    inventory
    .groupBy("ID")
    .agg(
        F.countDistinct(F.col("ELEMENT")).alias("ELEMENT_COUNT")
        )
    )
stationElements.show(10, False)

#How many stations collect all five core elements?
coreElement = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]
stationCoreElements = (
    inventory
    .filter(F.col("ELEMENT").isin(coreElement))
    .groupBy("ID")
    .agg(
        F.countDistinct(F.col("ELEMENT")).alias("CORE_ELEMENT_COUNT")
        )
    )
stationAllCoreElements =stationCoreElements.filter(F.col("CORE_ELEMENT_COUNT") == 5)
stationAllCoreElements.show(10, False)
stationAllCoreElements.count()
    
#How many only collected precipitation?
stationsOneElement = stationElements.filter(F.col("ELEMENT_COUNT") == 1)
print(stationsOneElement
    .join(inventory.filter(F.col("ELEMENT") == "PRCP"), on = "ID", how = "inner")
    .count()
)

#####################################################################################
# Processing Q3 (e)
#####################################################################################
#LEFT JOIN stations and output
stations = (
    stations
    .join(stationsActive, on = "ID", how = "left")
    .join(stationElements, on = "ID", how = "left")
    .join(stationCoreElements, on = "ID", how = "left")
    )
stations.show(10, False)

from pretty import SparkPretty 
pretty = SparkPretty()
print(pretty(stations.head()))

stations =stations.repartition(4)
stations.write.mode("overwrite").csv("hdfs:///user/xga37/outputs/ghcnd/stations")

#####################################################################################
# Processing Q3 (f)
#####################################################################################
#Are there any stations in your subset of daily that are not in stations at all?
temp = daily2021.join(stations, on = "ID", how = "left")
temp.filter(F.isnull(F.col("ELEMENT_COUNT"))).count()

#without using LEFT JOIN
print(daily2021.join(stations, on = "ID", how = "anti").count())


#####################################################################################









#####################################################################################
# Analysis Q1 (a)
#####################################################################################
#How many stations are there in total?
print(stations.count())

#How many stations were active in 2020?
print(stations.filter((F.col("START_YEAR") <= 2020) & (F.col("END_YEAR") >= 2020)).count())

#How many stations are in GSN?
print(stations.filter(F.col("GSN_FLAG") == "GSN").count())

#How many stations are in HCN?
print(stations.filter(F.col("HCN/CRN_FLAG") == "HCN").count())

#How many stations are in CRN?
print(stations.filter(F.col("HCN/CRN_FLAG") == "CRN").count())

#Are there any stations that are in more than one of these networks?
print(stations.filter((F.col("GSN_FLAG") != "") & (F.col("HCN/CRN_FLAG") != "")).count())

#####################################################################################
# Analysis Q1 (b)
#####################################################################################
#Count the total number of stations in each country
#Store the output in countries
stationsByCountry = stations.groupBy("COUNTRY_CODE").count()
countries = (
    countries
    .join(stationsByCountry, F.col("CODE") == F.col("COUNTRY_CODE"), how = "left")
    .drop("COUNTRY_CODE")
    .withColumnRenamed("count", "NUMBER OF STATIONS")
    )
countries.show(10, False)
countries = countries.repartition(2)
countries.write.mode("overwrite").csv("hdfs:///user/xga37/outputs/ghcnd/countries")

#Count the total number of stations in each state
#Store the output in states
stationsByState = stations.groupBy("STATE").count()
states = (
    states
    .join(stationsByState, F.col("CODE") == F.col("STATE"), how = "left")
    .drop("STATE")
    .withColumnRenamed("count", "NUMBER OF STATIONS")
    )
states.show(10, False)
states = states.repartition(2)
states.write.mode("overwrite").csv("hdfs:///user/xga37/outputs/ghcnd/states")

#####################################################################################
# Analysis Q1 (c)
#####################################################################################
#How many stations are there in the Southern Hemisphere only?
print(stations.filter(F.col("LATITUDE") < 0).count())

#How many stations are there in total in the territories of the United States
#around the world, excluding the United States itself?
print(
    stations
    .filter(
        (F.col("COUNTRY_NAME").contains("United States")) &
        (F.col("COUNTRY_CODE") != "US")
        )
    .count()
    )

#####################################################################################
# Analysis Q2 (a)
#####################################################################################
#Write a Spark function that computes the geographical distance between two stations
from math import sin, cos, sqrt, atan2, radians

def distance(X1, Y1, X2, Y2):
    R = 6373
    X1 = radians(X1)
    Y1 = radians(Y1)
    X2 = radians(X2)
    Y2 = radians(Y2)
    a = (sin((X2-X1)/2))**2 + cos(X1) * cos(X2) * (sin((Y2-Y1)/2))**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R*c

distance_udf = F.udf(distance,FloatType())

#####################################################################################
# Analysis Q2 (b)
#####################################################################################
#What two stations are geographically the closest together in New Zealand?
stationsNZ = (
    stations
    .filter(F.col("COUNTRY_CODE") == "NZ")
    .select("ID", "LATITUDE", "LONGITUDE")
    )
stationsNZ.cache()

pairs1 = (
    stationsNZ
    .select([F.col(name).alias(name + "1") for name in stationsNZ.columns])
    )
pairs2 = (
    stationsNZ
    .select([F.col(name).alias(name + "2") for name in stationsNZ.columns])
    )
stationsPairs = pairs1.crossJoin(pairs2).filter(F.col("ID1") != F.col("ID2"))
stationsPairs.show()

stationsDistanceNZ = (
    stationsPairs
    .withColumn("DISTANCE",
                distance_udf(
                    F.col("LATITUDE1").cast(FloatType()),
                    F.col("LONGITUDE1").cast(FloatType()),
                    F.col("LATITUDE2").cast(FloatType()),
                    F.col("LONGITUDE2").cast(FloatType())
                    )
                )
    .dropDuplicates(subset = ["DISTANCE"])
    .sort("DISTANCE")
    )
stationsDistanceNZ.show()
stationsDistanceNZ = stationsDistanceNZ.repartition(2)
stationsDistanceNZ.write.mode("overwrite").csv("hdfs:///user/xga37/outputs/ghcnd/distances")

#####################################################################################
# Analysis Q3 (b)
#####################################################################################
#Load and count the number of observations in 2015
daily2015 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("dateFormat", "yyyyMMdd")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2015.csv.gz")
)
print(daily2015.count())

#Load and count the number of observations in 2021
daily2021 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("dateFormat", "yyyyMMdd")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2021.csv.gz")
)
print(daily2021.count())

#####################################################################################
# Analysis Q3 (c)
#####################################################################################
#Load and count the number of observations from 2015 to 2021 (inclusive)
daily2015_2021 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("dateFormat", "yyyyMMdd")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/20{1[5-9],2[0-1]}*")
)
print(daily2015_2021.count())

#####################################################################################
# Analysis Q4 (a)
#####################################################################################
#Count the number of rows in daily
daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("dateFormat", "yyyyMMdd")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/*")
    .repartition(partitions)
    )
print(daily.count())

#####################################################################################
# Analysis Q4 (b)
#####################################################################################
#Filter daily to obtain the subset of observations containing the five core elements
#How many observations are there for each of the five core elements?
coreElement = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]
dailyCore = daily.filter(F.col("ELEMENT").isin(coreElement))

dailyCoreCount = dailyCore.groupBy("ELEMENT").count()
dailyCoreCount.show()

#####################################################################################
# Analysis Q4 (c)
#####################################################################################
#Determine how many observations of TMIN do not have a corresponding observation of TMAX
dailyT = (
    dailyCore
    .filter(F.col("ELEMENT").isin(["TMIN", "TMAX"]))
    .groupBy("ID", "DATE")
    .agg(
        F.collect_set("ELEMENT").alias("T")
        )
    )

dailyTMIN = dailyT.filter(F.array_contains(F.col("T"), "TMIN") & (F.size(F.col("T")) == 1))
print(dailyTMIN.count())

#How many different stations contributed to these observations?
dailyTMIN.select(F.countDistinct(F.col("ID"))).show()

#####################################################################################
# Analysis Q4 (d)
#####################################################################################
#Obtain all observations of TMIN and TMAX for all stations in New Zealand
#Save the result to your output directory
dailyTNZ = (
    stationsNZ
    .join(
        daily.filter(F.col("ELEMENT").isin(["TMIN", "TMAX"])),
        on = "ID",
        how = "inner"
        )
    )

dailyTNZ = dailyTNZ.repartition(1)
dailyTNZ.write.mode("overwrite").csv("hdfs:///user/xga37/outputs/ghcnd/dailyTNZ")

#How many observations
print(dailyTNZ.count())

#How many years are covered by the observations
dailyTNZ = dailyTNZ.withColumn("YEAR", F.substring(F.col("DATE"), 1, 4))
dailyTNZ.select(F.countDistinct(F.col("YEAR"))).show()

#copy the output from HDFS to local home directory
#count the number of rows in the part files using the wc -l bash command
!hdfs dfs -copyToLocal hdfs:///user/xga37/outputs/ghcnd/dailyTNZ /users/home/xga37/dailyTNZ
!wc -l /users/home/xga37/dailyTNZ

#####################################################################################
# Analysis Q4 (e)
#####################################################################################
#Group the precipitation observations by year and country
#Compute the average rainfall in each year for each country
#Which country has the highest average rainfall in a single year?
daily = daily.withColumn("YEAR", F.substring(F.col("DATE"), 1, 4))
avgRainByCountry = (
    daily
    .filter(F.col("ELEMENT") == "PRCP")
    .join(stations, on = "ID", how = "left")
    .groupBy("COUNTRY_CODE", "COUNTRY_NAME", "YEAR")
    .agg(F.mean("VALUE").alias("AVERAGE_RAINFALL"))
    .sort(F.col("AVERAGE_RAINFALL").desc())
    )
avgRainByCountry.show(10, False)

#Save this result to outputs
#copy the output from HDFS to local home directory
avgRainByCountry = avgRainByCountry.repartition(1)
avgRainByCountry.write.mode("overwrite").csv("hdfs:///user/xga37/outputs/ghcnd/rainfallByCountry")
!hdfs dfs -copyToLocal hdfs:///user/xga37/outputs/ghcnd/rainfallByCountry /users/home/xga37/rainfallByCountry









