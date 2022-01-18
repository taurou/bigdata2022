# Remove folders of the previous run
hdfs dfs -rm -r ex47_data
hdfs dfs -rm -r ex47_out

# Put input data collection into hdfs
hdfs dfs -put ex47_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise47.SparkDriver --deploy-mode client --master yarn target/Exercise47_DataFrame-1.0.0.jar "ex47_data/persons.csv" "ex47_out/"


