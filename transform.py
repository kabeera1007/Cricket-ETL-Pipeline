import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, current_timestamp, to_date, split
from pyspark.sql.types import DecimalType
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Spark context and GlueContext
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(sc)
job = Job(glueContext)

access_key = ' ' # Put your access key here
secret_key = ' ' # Put your secret key here
aws_region = ' '   # Put your region here

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")

# Path to the raw data in S3
raw_data_path = "s3://crickdatabucket/raw_data/to_processed/"

# Reading the raw data from S3
raw_df = spark.read.json(raw_data_path)

# Transformation functions
def summary(raw_df):
    summary_df = raw_df.select(
        date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss').alias("Timestamp"),
        col("match_details.series").alias("Series"),
        col("match_details.series_id").alias("Series_Id"),
        col("match_details.series_type").alias("Series_Type"),
        col("match_details.venue").alias("Venue"),
        col("match_details.venue_id").alias("Venue_Id"),
        col("match_details.matchs").alias("Match"),
        col("match_details.match_id").alias("Match_Id"),
        to_timestamp(col("match_details.date_time"), 'yyyy-MM-dd HH:mm:ss').alias("Match_Timestamp"),
        to_date(col("Match_Timestamp")).alias("Match_Date"),
        col("match_details.match_time").alias("Match_Time"),
        col("match_details.match_type").alias("Match_Type"),
        col("match_details.toss").alias("Toss"),
        col("match_details.balling_team").cast("long").alias("Balling_Team_Id"),
        col("match_details.batting_team").cast("long").alias("Batting_Team_Id"),
    )
    return summary_df

def team_a(raw_df):
    team_a_df = raw_df.select(
        date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss').alias("Timestamp"),
        col("match_details.team_a_id").alias("Team_A_Id"),
        col("match_details.team_a_img").alias("Team_A_Image"),
        col("match_details.team_a").alias("Team_A_Name"),
        col("match_details.team_a_short").alias("Team_A_Shortform"),
        col("match_details.team_a_scores").alias("Team_A_Scoreboard"),
        col("match_details.team_a_over").cast(DecimalType(10, 1)).alias("Team_A_Over"),
        split(col("Team_A_Scoreboard"), "-").getItem(0).cast("long").alias("Team_A_Score"),
        split(col("Team_A_Scoreboard"), "-").getItem(1).cast("long").alias("Team_A_Wicket")
    )
    return team_a_df

def team_b(raw_df):
    team_b_df = raw_df.select(
        date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss').alias("Timestamp"),
        col("match_details.team_b_id").alias("Team_B_Id"),
        col("match_details.team_b_img").alias("Team_B_Image"),
        col("match_details.team_b").alias("Team_B_Name"),
        col("match_details.team_b_short").alias("Team_B_Shortform"),
        col("match_details.team_b_scores").alias("Team_B_Scoreboard"),
        col("match_details.team_b_over").cast(DecimalType(10, 1)).alias("Team_B_Over"),
        split(col("Team_B_Scoreboard"), "-").getItem(0).cast("long").alias("Team_B_Score"),
        split(col("Team_B_Scoreboard"), "-").getItem(1).cast("long").alias("Team_B_Wicket")
    )
    return team_b_df

def status(raw_df):
    status_df = raw_df.select(
        date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss').alias("Timestamp"),
        col("match_details.match_status").alias("Match_Status"),
        col("match_details.team_a_id").alias("Team_A_Id"),
        col("match_details.team_a").alias("Team_A_Name"),
        col("match_details.team_a_over").cast(DecimalType(10, 1)).alias("Team_a_Over"),
        col("match_details.team_b_id").alias("Team_B_Id"),
        col("match_details.team_b").alias("Team_B_Name"),
        col("match_details.team_b_over").cast(DecimalType(10, 1)).alias("Team_B_Over"),
        col("match_details.trail_lead").alias("Trail_Lead"),
        col("match_details.session").alias("Session"),
        col("match_details.need_run_ball").alias("needed"),
        col("match_details.result").alias("Result")
    )
    return status_df

def commentary(raw_df):
    commentary_df = raw_df.select(
        date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss').alias("Timestamp"),
        col("match_details.team_a_over").cast(DecimalType(10, 1)).alias("Team_A_Over"),
        col("match_details.team_b_over").cast(DecimalType(10, 1)).alias("Team_B_Over"),
        col("player_details.commentary").alias("Commentary")  # Fix applied here
    )
    return commentary_df

def batsmen(raw_df):
    batsmen_df = raw_df.select(
        date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss').alias("Timestamp"),
        col("match_details.team_a_over").cast(DecimalType(10, 1)).alias("Team_A_Over"),
        col("match_details.team_b_over").cast(DecimalType(10, 1)).alias("Team_B_Over"),
        col("player_details.0").alias("Batsmen_Name"),
        col("player_details.1").cast("long").alias("Runs"),
        col("player_details.2").cast("long").alias("Balls"),
        col("player_details.3").cast("long").alias("Four"),
        col("player_details.4").cast("long").alias("Six"),
        col("player_details.5").cast(DecimalType(10, 2)).alias("Strike_Rate")
    )
    return batsmen_df

def bowler(raw_df):
    bowler_df = raw_df.select(
        date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss').alias("Timestamp"),
        col("match_details.team_a_over").cast(DecimalType(10, 1)).alias("Team_A_Over"),
        col("match_details.team_b_over").cast(DecimalType(10, 1)).alias("Team_B_Over"),
        col("player_details.16").alias("Bowler_Name"),
        col("player_details.17").cast(DecimalType(10, 1)).alias("Bowler_Overs"),
        col("player_details.18").cast("long").alias("Maiden"),
        col("player_details.19").cast("long").alias("Bowler_Runs"),
        col("player_details.20").cast("long").alias("Wickets"),
        col("player_details.21").cast(DecimalType(10, 2)).alias("Economy")
    )
    return bowler_df



# Running the transformation functions
summary_df = summary(raw_df)
team_a_df = team_a(raw_df)
team_b_df = team_b(raw_df)
status_df = status(raw_df)
commentary_df = commentary(raw_df)
batsman_df = batsmen(raw_df)
bowler_df = bowler(raw_df)

# Writing the results to S3
summary_df.write.mode("overwrite").option("header", "true").csv("s3://crickdatabucket/transformed_data//summary/")
team_a_df.write.mode("overwrite").option("header", "true").csv("s3://crickdatabucket/transformed_data/team_a/")
team_b_df.write.mode("overwrite").option("header", "true").csv("s3://crickdatabucket/transformed_data/team_b/")
status_df.write.mode("overwrite").option("header", "true").csv("s3://crickdatabucket/transformed_data/status/")
commentary_df.write.mode("overwrite").option("header", "true").csv("s3://crickdatabucket/transformed_data/commentary/")
batsman_df.write.mode("overwrite").option("header", "true").csv("s3://crickdatabucket/transformed_data/batsman/")
bowler_df.write.mode("overwrite").option("header", "true").csv("s3://crickdatabucket/transformed_data/bowler/")

# Commit job in Glue
job.commit()