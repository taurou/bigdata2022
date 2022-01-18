package it.polito.bigdata.spark.exercise48;

import org.apache.spark.sql.Dataset;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;





public class SparkDriver {

	// Select male users (gender=“male”), increase by one their age, and store
	// in the output folder name and age of these users sorted by decreasing age
	// and ascending name (if the age value is the same)

	public static void main(String[] args) {
		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF); 


		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder().appName("Spark Exercise #48 - DataFrame").getOrCreate();
		SparkSession ss = SparkSession.builder().master("local").appName("Spark Exercise #48 - DataFrame").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: name,age,gender
		// The input file has an header. Hence, the name of the columns of
		// the defined DataFrame will be name, age, gender
		Dataset<Row> dfProfiles = ss.read().format("csv")
				.option("header", true).option("inferSchema", true)
				.load(inputPath);
		
		
		// printSchema and show only for debug purposes
		// They can be removed
		dfProfiles.printSchema();
		dfProfiles.show();

		// Compute avg(age) and count(*) for each name
		Dataset<Row> dfNameAvgAgeCount = dfProfiles.groupBy("name")
				.agg(avg("age"),count("*"))
				.withColumnRenamed("avg(age)", "avgage")
				.withColumnRenamed("count(1)", "count");

		// Select the names with at least two occurrences 
		Dataset<Row> dfSeletedRecords = dfNameAvgAgeCount.filter("count>=2");
		
		// Select only columns name and avg(age)
		Dataset<Row> dfSeletedNames = dfSeletedRecords.select("name", "avgage");
		
		// printSchema and show only for debug purposes
		// They can be removed
		dfSeletedNames.printSchema();
		dfSeletedNames.show();
		
		
		
		dfSeletedNames.write().format("csv").option("header", false).save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
