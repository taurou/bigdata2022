# Remove folders of the previous run
hdfs dfs -rm -r ex32_data

# Put input data collection into hdfs
hdfs dfs -put ex32_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise32.SparkDriver --deploy-mode client --master yarn target/Exercise32Top-1.0.0.jar "ex32_data/sensors.txt"


