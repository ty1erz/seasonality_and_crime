import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._

val spark = SparkSession.builder.appName("CSV Reader").getOrCreate()
val options = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")
val df = spark.read.options(options).csv("hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/Chi_Crimes_2001_to_Present.csv")

var df_clean = df.drop("ID", "Case Number", "Block", "IUCR", "Location Description", "Arrest", "Domestic", "Beat", "District", "Ward", "Community Area", "FBI Code", "X Coordinate", "Y Coordinate", "Updated On", "Latitude", "Longitude", "Location")

val format = "MM/dd/yyyy hh:mm:ss a"
df_clean = df_clean.withColumn("Date", to_date(col("Date"), format))
df_clean = df_clean.na.drop()

df_clean.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("temp")

val fs = FileSystem.get(sc.hadoopConfiguration)
val file = fs.globStatus(new Path("temp/part*"))(0).getPath().getName()
fs.rename(new Path("temp/" + file), new Path("hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/crime_type_date.csv"))
fs.delete(new Path("temp"), true)
