val df1 = spark.read.option("header",true).csv("hdfs://nyu-dataproc-m/user/tz2076_nyu_edu/final_project/data/nypd_raw_data.csv")
val ds = df1.map(row=>{(row.getString(0), row.getString(1), row.getString(4),row.getString(5))})
var dfCleaned = ds.toDF("id","date","typeId","typeDesc")
dfCleaned = dfCleaned.na.drop("any")

// data saving
dfCleaned = dfCleaned.coalesce(1)
dfCleaned.write.format("csv").mode("overwrite").option("header", "true").save("temp")
import org.apache.hadoop.fs._
var fs = FileSystem.get(sc.hadoopConfiguration)
var file = fs.globStatus(new Path("temp/part*"))(0).getPath().getName()

fs.rename(new Path("temp/" + file), new Path("hdfs://nyu-dataproc-m/user/tz2076_nyu_edu/final_project/data/nypd_cleaned.csv"))
fs.delete(new Path("temp"), true)

var df = spark.read.option("header",true).csv("hdfs://nyu-dataproc-m/user/tz2076_nyu_edu/final_project/data/nypd_cleaned.csv")
df = df.withColumn("date",to_date(col("date"),"MM/dd/yyyy"))
df = df.withColumn("id", col("id").cast("int"))
df = df.withColumn("typeId", col("typeId").cast("int"))

val df_assault = df.filter(col("typeDesc").rlike(".*ASSAULT.*"))
val df_larceny = df.filter(col("typeDesc").rlike(".*LARCENY.*"))

val df_profiled_all = df.groupBy("date").count().sort("date").coalesce(1)
val df_profiled_assault = df_assault.groupBy("date").count().sort("date").coalesce(1)
val df_profiled_larceny = df_larceny.groupBy("date").count().sort("date").coalesce(1)

// data saving
import org.apache.hadoop.fs._
df_profiled_all.write.format("csv").mode("overwrite").option("header", "true").save("temp")
fs = FileSystem.get(sc.hadoopConfiguration)
file = fs.globStatus(new Path("temp/part*"))(0).getPath().getName()
fs.rename(new Path("temp/" + file), new Path("final_project/data/nypd_all.csv"))
fs.delete(new Path("temp"), true)

df_profiled_assault.write.format("csv").mode("overwrite").option("header", "true").save("temp")
fs = FileSystem.get(sc.hadoopConfiguration)
file = fs.globStatus(new Path("temp/part*"))(0).getPath().getName()
fs.rename(new Path("temp/" + file), new Path("final_project/data/nypd_assault.csv"))
fs.delete(new Path("temp"), true)

df_profiled_larceny.write.format("csv").mode("overwrite").option("header", "true").save("temp")
fs = FileSystem.get(sc.hadoopConfiguration)
file = fs.globStatus(new Path("temp/part*"))(0).getPath().getName()
fs.rename(new Path("temp/" + file), new Path("final_project/data/nypd_larceny.csv"))
fs.delete(new Path("temp"), true)