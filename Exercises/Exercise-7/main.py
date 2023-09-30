from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from zipfile import ZipFile
import os
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    # your code here
    # The first we have to do is unzip the file 
    # so we create a variable with the path of the csv 
    file_path_csv = "data\hard-drive-2022-01-01-failures.csv"
    # Another var with the path of the zip folder
    zip_folder = "data\hard-drive-2022-01-01-failures.csv.zip"
    # we use the module zipfile to unzip the file
    with ZipFile(zip_folder, 'r') as o:
        # with the method extrall we can extract all files inside the zip
        o.extractall("data")
    
    # 1. Add the file name as a column to the DataFrame and call it `source_file`.
    # To read the csv on pyspark we use spark.read.csv
    df = spark.read.option("header","true").csv(file_path_csv)
    # we need to obtain the name of the file, so we use the function "basename" in the parameter we write the path of the csv
    source_file = os.path.basename(file_path_csv)
    # use the function splitext to get just hard-drive-2022-01-01-failures.csv
    source_file = os.path.splitext(source_file)[0]
    # at least we create a new column with the name source_file and we fill in all rows with the name of file
    df = df.withColumn("source_file",F.lit(source_file))

    # 2. Pull the `date` located inside the string of the `source_file` column. Final data-type must be 
    # `date` or `timestamp`, not a `string`. Call the new column `file_date`.

    # This part is this part is a bit complex to understand
    # let's take it one step at a time
    # First of all we have to extract the date of the column 'source_file'
    # we should extract the year, month and day of this string 'hard-drive-2022-01-01-failures'
    # so for each extract we are going to create a new column and after we are going to concate the 3 columns
    # if we want to extract the year we have to use the funciton spit
    # watching the string, the best way to split is whith the character '-'
    # then we have to count the number of '-' till we get the year
    # could be like this -> position 0 = hard     position 1 = drive   position 2 = 2022
    # we get it, the position 2 is the year. So we have to split and get item the position 2 
    df = df.withColumn('yy',F.split(df.source_file, '-').getItem(2))
    # the same logic with the months
    df = df.withColumn('mm',F.split(df.source_file, '-').getItem(3))
    # the same logic with the days
    df = df.withColumn('dd',F.split(df.source_file, '-').getItem(4))
    # we have 3 new columns with the year, month and days
    # we have to concat these 3 columns in 1 column
    # Then we are going to create antoher colum and with the function concat_ws, the strings of these 3 columns are going to concate
    df = df.withColumn('date_entera',F.concat_ws('-',df.yy,df.mm,df.dd))
    # we gotta, now we should drop these 3 columns to optimize memory
    df = df.drop('yy')
    df = df.drop('mm')
    df = df.drop('dd')
    # At least we have to cast the column date_entera string -> date. 
    # Pyspark have types of cast, one of them is Datetype
    df_date = df.select(F.col('date_entera'), F.to_date(F.col('date_entera').cast(DateType())).alias("date"))

    # 3. Add a new column called `brand`. It will be based on the column `model`. If the
    # column `model` has a space ... aka ` ` in it, split on that `space`. The value
    # found before the space ` ` will be considered the `brand`. If there is no
    # space to split on, fill in a value called `unknown` for the `brand`.
   
    # Define a UDF to extract the brand
    def extract_brand(model):
        if ' ' in model:
            return model.split(' ')[0]
        else:
            return 'unknown'
    
    # Register the UDF
    extract_brand_udf = udf(extract_brand, StringType())

    # Add a new 'brand' column using the UDF
    df_testing = df.withColumn('brand', extract_brand_udf(df['model']))
    #df_testing.select('brand').show()

    # 4. Inspect a column called `capacity_bytes`. Create a secondary DataFrame that
    # relates `capacity_bytes` to the `model` column, create "buckets" / "rankings" for
    # those models with the most capacity to the least. Bring back that 
    # data as a column called `storage_ranking` into the main dataset.

    # Create a secondary DataFrame with model and capacity_bytes

    secondary_df = df.select('model','capacity_bytes')

    # Define a Window specification to rank models by capacity in descending order

    window_spec  = Window.orderBy(F.desc('capacity_bytes'))

    # Add a 'storage_ranking' column to the secondary DataFrame

    secondary_df  = secondary_df.withColumn('storage_ranking',F.rank().over(window_spec ))

    # Join the secondary DataFrame back to the main DataFrame using 'model' as the key

    secondary_df = secondary_df.join(secondary_df, 'model','left')
    #secondary_df.show()

    # 5. Create a column called `primary_key` that is `hash` of columns that make a record umique
    #in this dataset.

    # Create a new 'primary_key' column by hashing the unique columns
    df5 = df.withColumn('primary_key', F.sha1(F.concat_ws('|',F.monotonically_increasing_id())))
    df5.select('primary_key').show()

if __name__ == "__main__":
    main()

