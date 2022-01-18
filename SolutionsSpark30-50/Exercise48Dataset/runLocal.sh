rm -rf ex48_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise48.SparkDriver --deploy-mode client --master local target/Exercise48_Dataset-1.0.0.jar "ex48_data/persons.csv" "ex48_out/"


