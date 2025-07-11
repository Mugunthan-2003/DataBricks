import pyspark.sql.functions as f

df=spark.table("testing.hp.hp1_bronze")
cols=["mainroad","guestroom","basement","hotwaterheating","airconditioning","prefarea"]
df=df.replace({"no":"0","yes":"1"},subset=cols)
df=df.replace({"unfurnished":"0","semi-furnished":"1","furnished":"2"},subset=["furnishingstatus"])
cols+=["furnishingstatus"]
for c in cols:
    df=df.withColumn(c,f.col(c).cast("int"))
df=df.withColumnsRenamed({'price':'Price',
 'area':'Area',
 'bedrooms':'Bedrooms',
 'bathrooms':'Bathrooms',
 'stories':'Stories',
 'mainroad':'Main_Road',
 'guestroom':'Guest_Room',
 'basement':'Basement',
 'hotwaterheating':'Hot_Water_Heating',
 'airconditioning':'Air_Conditioning',
 'parking':'Parking',
 'prefarea':'Pref_Area',
 'furnishingstatus':'Furnishing_Status'})

#df.show(5)
df.write.mode("overwrite").saveAsTable("testing.hp.hp1_silver")