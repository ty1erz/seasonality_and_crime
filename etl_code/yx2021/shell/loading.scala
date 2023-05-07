// Start Shell
spark-shell --deploy-mode client
// Import data and load to a dataframe
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("CSV Reader").getOrCreate()
val options = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")
val df = spark.read.options(options).csv("hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/Chi_Crimes_2001_to_Present.csv")
// Show dataframe schema and first several rows
df.printSchema()
df.show()