#in bronze notebook run
# copy one patient info from the table , then update ,age or anything else 
# make sure to update to more recent time,for it to appear in gold
manual_json= '{"patient_id": "94f2aeb0-e969-4564-982a-9ca41efcc9e0", "gender": "Female", "age": 31, "department": "Emergency", "admission_time": "2025-09-07T01:24:45.898668", "discharge_time": "2025-09-09T08:25:45.898668", "bed_id": 284, "hospital_id": 1}'

manual_df = spark.createDataFrame([manual_json], ["raw_json"])
manual_df.write.format("delta").mode("append").save(bronze_path)

display(spark.read.format("delta").load(bronze_path))

#in silver notebook run 
# run the main job again ,check display to observe new row added

display(spark.read.format("delta").load(silver_path))

#in gold notebook run , filter to check the added patient id
#first run main code then the filter
#filter the SCD2 patient with id 94f2aeb0-e969-4564-982a-9ca41efcc9e0
display(spark.read.format("delta").load(gold_dim_patient).filter("patient_id = '94f2aeb0-e969-4564-982a-9ca41efcc9e0'"))
