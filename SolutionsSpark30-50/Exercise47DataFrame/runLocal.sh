rm -rf ex47_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise48.SparkDriver --deploy-mode client --master local target/Exercise47_DataFrame-1.0.0.jar "ex47_data/persons.csv" "ex47_out/"


