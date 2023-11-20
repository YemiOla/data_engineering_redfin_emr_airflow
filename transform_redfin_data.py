from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Create a spark session
spark = SparkSession.builder.appName("RedfinDataAnalysis").getOrCreate()

def transform_date():
    raw_data_s3_bucket = "s3://redfin-data-project-yml/store-raw-data-yml/city_market_tracker.tsv000.gz"
    transform_data_s3_bucket = "s3://redfin-data-project-yml/redfin-transform-zone-yml/redfin_data.parquet"
    #Read the redfin data from the s3 bucket
    redfin_data = spark.read.csv(raw_data_s3_bucket, header=True, inferSchema=True, sep= "\t")

    #Select only specific columns
    df_redfin = redfin_data.select(['period_end','period_duration', 'city', 'state', 'property_type',
        'median_sale_price', 'median_ppsf', 'homes_sold', 'inventory', 'months_of_supply', 'median_dom', 'sold_above_list', 'last_updated'])


    #remove na 
    df_redfin = df_redfin.na.drop()

    #Extract year from period_end and save in a new column "period_end_yr"
    df_redfin = df_redfin.withColumn("period_end_yr", year(col("period_end")))

    #Extract month from period_end and save in a new column "period_end_month"
    df_redfin = df_redfin.withColumn("period_end_month", month(col("period_end")))

    # Drop period_end and last_updated columns
    df_redfin = df_redfin.drop("period_end", "last_updated")

    #let's map the month number to their respective month name.

    df_redfin = df_redfin.withColumn("period_end_month", 
                    when(col("period_end_month") == 1, "January")
                    .when(col("period_end_month") == 2, "February")
                    .when(col("period_end_month") == 3, "March")
                    .when(col("period_end_month") == 4, "April")
                    .when(col("period_end_month") == 5, "May")
                    .when(col("period_end_month") == 6, "June")
                    .when(col("period_end_month") == 7, "July")
                    .when(col("period_end_month") == 8, "August")
                    .when(col("period_end_month") == 9, "September")
                    .when(col("period_end_month") == 10, "October")
                    .when(col("period_end_month") == 11, "November")
                    .when(col("period_end_month") == 12, "December")
                    .otherwise("Unknown")
                    )


    #let us write the final dataframe into our s3 bucket as a parquet file.    
    df_redfin.write.mode("overwrite").parquet(transform_data_s3_bucket)

transform_date()