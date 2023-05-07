// drop unnecessary columns
var df_clean = df.drop("ID", "Case Number", "Block", "IUCR", "Location Description", "Arrest", "Domestic", "Beat", "District", "Ward", "Community Area", "FBI Code", "X Coordinate", "Y Coordinate", "Updated On", "Latitude", "Longitude", "Location")
df_clean.printSchema()
df_clean.show()
// convert date column to date object
val format = "MM/dd/yyyy hh:mm:ss a"
df_clean = df_clean.withColumn("Date", to_date(col("Date"), format))
df_clean.printSchema()
df_clean.show()
// drop row contain NA value
df_clean = df_clean.na.drop()
df_clean.count()
// output result to a csv file
val outputPath: String = "hdfs://nyu-dataproc-m/user/yx2021_nyu_edu/final_project/data/crime_type_date.csv"
df_clean.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(outputPath)