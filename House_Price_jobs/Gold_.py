import pyspark.sql.functions as f

df=spark.table("testing.hp.hp1_silver")
#df.printschema()

df=df.filter(
    (df.Main_Road ==1) &
    (df.Guest_Room ==1) &
    (df.Hot_Water_Heating ==1) &
    (df.Air_Conditioning ==1) &
    (df.Pref_Area ==1) &
    (df.Furnishing_Status ==3)
)
df = df.agg(
    f.count("*").alias("No_of_House_with_all_facility"))
df.write.mode("overwrite").saveAsTable("testing.hp.hp1_gold_number")