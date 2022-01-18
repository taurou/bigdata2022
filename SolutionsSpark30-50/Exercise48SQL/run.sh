# Remove folders of the previous run
hdfs dfs -rm -r ex48_data
hdfs dfs -rm -r ex48_out

# Put input data collection into hdfs
hdfs dfs -put ex48_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise32.SparkDriver --deploy-mode client --master yarn target/Exercise48_SQL-1.0.0.jar "ex48_data/persons.csv" "ex48_out/"


