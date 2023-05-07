import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._

val spark = SparkSession.builder.appName("CSV Reader").getOrCreate()
val options = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")
val df = spark.read.options(options).csv("hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/crime_type_date.csv")
val fs = FileSystem.get(sc.hadoopConfiguration)

var crime_occurrence_per_day = df.groupBy("Date").agg(count("*").as("Count")).orderBy("Date")
crime_occurrence_per_day.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("temp")

var file = fs.globStatus(new Path("temp/part*"))(0).getPath().getName()
fs.rename(new Path("temp/" + file), new Path("hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/crime_occurrence_per_day.csv"))
fs.delete(new Path("temp"), true)

val df_theft = df.filter(col("Primary Type") === "THEFT")
var theft_occurrence_per_day = df_theft.groupBy("Date").agg(count("*").as("Count")).orderBy("Date")
theft_occurrence_per_day.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("temp")

file = fs.globStatus(new Path("temp/part*"))(0).getPath().getName()
fs.rename(new Path("temp/" + file), new Path("hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/theft_occurrence_per_day.csv"))
fs.delete(new Path("temp"), true)

val df_battery = df.filter(col("Primary Type") === "BATTERY")
var battery_occurrence_per_day = df_battery.groupBy("Date").agg(count("*").as("Count")).orderBy("Date")
battery_occurrence_per_day.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("temp")

file = fs.globStatus(new Path("temp/part*"))(0).getPath().getName()
fs.rename(new Path("temp/" + file), new Path("hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/battery_occurrence_per_day.csv"))
fs.delete(new Path("temp"), true)
