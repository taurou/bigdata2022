# Remove folders of the previous run
hdfs dfs -rm -r ex46_data

hdfs dfs -rm -r ex46_out

# Put input data collection into hdfs
hdfs dfs -put ex46_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise46.SparkDriver --deploy-mode cluster --master yarn target/Exercise46-1.0.0.jar "ex46_data/readings.txt" ex46_out

