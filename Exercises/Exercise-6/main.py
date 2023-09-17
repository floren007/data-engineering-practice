from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from zipfile import ZipFile
import os
from pyspark.sql.types import IntegerType,BooleanType,DateType

def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    
    # your code here
    # read the dsv with spark
    with ZipFile("data\Divvy_Trips_2019_Q4.zip",'r') as zip:
        zip.extractall()
    df=spark.read.csv("Divvy_Trips_2019_Q4.csv",header=True,inferSchema=True)
    # 1.- make the average of the column trip duraiton
    df.select(F.avg('tripduration')).show()
    # 2.- How many trips were taken each day?
    # To know how many trips do each day, we have to look the column of start time, the column contain the date and the hour
    # but the problem says "the trips each day", so we have a problem with the hour. To solve it we will use the function split()
    # to split te date and the hour, then we can get just the date
    df_trips = df.withColumn('start_time',F.split(F.col('start_time'), " ").getItem(0))
    # We have the split, then group the dates and colunts how many flights.. Solve it ;)
    trips_each_day = df_trips.groupBy('start_time').agg(F.count("*").alias("trips_each_day"))
    trips_each_day.show()
    # 3.- What was the most popular starting trip station for each month?
    # For this exercise we have to do the same than the last exercise, split the month
    most_pop_month = df_trips.withColumn('start_time',F.split(F.col('start_time'),"-").getItem(1))
    # we have the month and now we have to know what is the best month, then we use the function max() 
    # that print the month with more numbers
    most_popular_month = most_pop_month.agg(F.max('start_time').alias("popular_month"))
    most_popular_month.show()
    # 4.- What were the top 3 trip stations each day for the last two weeks?
    # split the date to obtain just days month and year
    df_trips = df.withColumn('start_time',F.split(F.col('start_time'), " ").getItem(0))
    # filter the date for the last 2 weeks
    df_trips = df_trips.filter(F.col('start_time') >= "2019-12-15")
    # group the name stations for dates and count the number of flights
    stations_counts = df_trips.groupBy('start_time','from_station_name').count()
    # order the count the highet to lower and we get the first 3 rows
    top3 = stations_counts.orderBy(F.col("count").desc())
    top3.show(3)

    # 5. Do `Male`s or `Female`s take longer trips on average?
    # This is exercise is the easier, only need knowledge of sql
    # filter the dataframe just for males
    dFilterMan = df.select("*").where(df.gender == "Male")
    # then make the average with the column tripduration
    dFilterMan = dFilterMan.select(F.avg('tripduration').alias("average_trip_male")).show()
    # with woman is njust the same but filter for female
    dfFilterWoman = df.select("*").where(df.gender == "Female")
    dfFilterWoman = dfFilterWoman.select(F.avg('tripduration').alias("average_trip_female")).show()
    # so we can say the woman take longer trips on average

    # 6. What is the top 10 ages of those that take the longest trips, and shortest?
    # as first we have to show the type data of column tripduration, check that is string and the values of tripduration are in seconds
    # so we should cast them to minutes, to easy read. With an expression we can change them to minutes
    cast_trip_duration = df.withColumn("tripduration", F.expr("CAST(REPLACE(tripduration, ',', '') AS FLOAT) / 60"))
    # group them with the column birthyear and max the values of each year
    dfFilterAge = cast_trip_duration.select("*").groupBy("birthyear").agg(F.max("tripduration").alias("longer_trips"))
    # at least order the longest trip higer to shortest
    dfFilterAge_max = dfFilterAge.orderBy(F.col("longer_trips").desc())
    dfFilterAge_max.show(10)
    # repeat the same logic with for the shortest trip
    dfFilterAge_min = cast_trip_duration.select("*").groupBy("birthyear").agg(F.min("tripduration").alias("shortest_trips"))
    dfFilterAge_min = dfFilterAge_min.orderBy(F.col("shortest_trips").desc())
    dfFilterAge_min.show(10)
   
  
if __name__ == "__main__":
    main()
