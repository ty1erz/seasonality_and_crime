var df = spark.read.option("header",true).csv("final_project/data/nypd_cleaned.csv")
df.show()

df = df.withColumn("date",to_date(col("date"),"MM/dd/yyyy"))
df = df.withColumn("id", col("id").cast("int"))
df = df.withColumn("typeId", col("typeId").cast("int"))

df.printSchema()

//create extra column that tells which week it was in.
df = df.withColumn("week_number", weekofyear(col("date")))

df.show()
df.printSchema()

//find distinct values of crime type and date
val distinct_crime_type = df.select(col("typeId")).distinct()
val distinct_date = df.select(col("date")).distinct()
distinct_crime_type.collect()
distinct_crime_type.count()
distinct_date.collect()
distinct_date.count()


//get mean of week number
val mean_week: Double = df.select(mean("week_number")).collect()(0)(0).asInstanceOf[Double]

df.groupBy("typeId","typeDesc").count().alias("count").filter(col("count") > 1000).sort(desc("count")).show()