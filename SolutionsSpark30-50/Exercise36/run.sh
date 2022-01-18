# Remove folders of the previous run
hdfs dfs -rm -r ex36_data

# Put input data collection into hdfs
hdfs dfs -put ex36_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise36.SparkDriver --deploy-mode cluster --master yarn target/Exercise36-1.0.0.jar "ex36_data/sensors.txt"


