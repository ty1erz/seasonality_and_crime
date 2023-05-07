// Start Shell
// spark-shell --deploy-mode client
// Import data and load to a dataframe
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("CSV Reader").getOrCreate()
val options = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")
val df = spark.read.options(options).csv("hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/Chi_Crimes_2001_to_Present.csv")
df.printSchema()
df.show()

/* Data Schema
root
 |-- ID: integer (nullable = true)
 |-- Case Number: string (nullable = true) \\ not useful
 |-- Date: string (nullable = true)
 |-- Block: string (nullable = true) \\ not useful
 |-- IUCR: string (nullable = true) \\ unique combination of primary type and secondary type
 |-- Primary Type: string (nullable = true) 
 |-- Description: string (nullable = true) 
 |-- Location Description: string (nullable = true)
 |-- Arrest: boolean (nullable = true)
 |-- Domestic: boolean (nullable = true)
 |-- Beat: integer (nullable = true)
 |-- District: integer (nullable = true)
 |-- Ward: integer (nullable = true)
 |-- Community Area: integer (nullable = true)
 |-- FBI Code: string (nullable = true)
 |-- X Coordinate: integer (nullable = true)
 |-- Y Coordinate: integer (nullable = true)
 |-- Year: integer (nullable = true)
 |-- Updated On: string (nullable = true)
 |-- Latitude: double (nullable = true)
 |-- Longitude: double (nullable = true)
 |-- Location: string (nullable = true)
*/

var df_clean = df.drop("Case Number", "Block", "Location Description", "Beat", "District", "Ward", "Community Area", "Latitude", "Longitude", "year")
df_clean.printSchema()
df_clean.show()
/* Cleaned Data Schema
root
 |-- ID: integer (nullable = true)
 |-- Date: date (nullable = true)
 |-- IUCR: string (nullable = true)
 |-- Primary Type: string (nullable = true)
 |-- Description: string (nullable = true)
 |-- Arrest: boolean (nullable = true)
 |-- Domestic: boolean (nullable = true)
 |-- FBI Code: string (nullable = true)
 |-- X Coordinate: integer (nullable = true)
 |-- Y Coordinate: integer (nullable = true)
 |-- Updated On: date (nullable = true)
 |-- Location: string (nullable = true)
*/

// Include initial code analysis. 
// Count distinct values in columns
df_clean.select("ID").distinct().count()
df_clean.select("Date").distinct().count()
df_clean.select("IUCR").distinct().count()
df_clean.select("Description").distinct().count()
df_clean.select("X Coordinate").distinct().count()
df_clean.select("Y Coordinate").distinct().count()
df_clean.select("Updated On").distinct().count()
df_clean.select("Location").distinct().count()
// Find distinct values in columns
df_clean.select("Primary Type").distinct().collect().foreach(row => println(row))
df_clean.select("Arrest").distinct().collect().foreach(row => println(row))
df_clean.select("Domestic").distinct().collect().foreach(row => println(row))
df_clean.select("FBI Code").distinct().collect().foreach(row => println(row))

