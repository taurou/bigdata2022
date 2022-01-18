rm -rf ex47_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise47.SparkDriver --deploy-mode client --master local target/Exercise47_Dataset-1.0.0.jar "ex47_data/persons.csv" "ex47_out/"


