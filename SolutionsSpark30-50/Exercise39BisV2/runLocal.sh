# Remove folders of the previous run
rm -rf ex39_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise39bis.SparkDriver --deploy-mode client --master local target/Exercise39BisV2-1.0.0.jar "ex39_data/sensors.txt" ex39_out


