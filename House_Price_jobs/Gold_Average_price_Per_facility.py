import pyspark.sql.functions as f

df=spark.table("testing.hp.hp1_silver")
df=df.groupBy("Bedrooms","Bathrooms").agg(f.avg("Price").alias("AveragePrice")).orderBy("Bedrooms","Bathrooms")
df.write.mode("overwrite").saveAsTable("testing.hp.hp1_gold_Averageprice")