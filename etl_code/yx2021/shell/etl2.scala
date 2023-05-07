// crime occurrence per day
var crime_occurrence_per_day = df_clean.groupBy("Date").agg(count("*").as("Count")).orderBy("Date")
crime_occurrence_per_day.show()
var outputPath: String = "hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/crime_occurrence_per_day"
crime_occurrence_per_day.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(outputPath)
// theft crime occurrence per day
var outputPath: String = "hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/theft_occurrence_per_day"
val df_clean_theft = df_clean.filter(col("Primary Type") === "THEFT")
df_clean_theft.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(outputPath)
// battery crime occurrence per day
var outputPath: String = "hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/battery_occurrence_per_day"
val df_clean_battery = df_clean.filter(col("Primary Type") === "BATTERY")
df_clean_battery.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(outputPath)