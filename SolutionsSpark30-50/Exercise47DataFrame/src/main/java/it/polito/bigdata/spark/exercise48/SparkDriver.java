package it.polito.bigdata.spark.exercise48;

import org.apache.spark.sql.Dataset;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
		// SparkSession ss = SparkSession.builder()
				//.appName("Spark Exercise #48 - DataFrame").getOrCreate();
		SparkSession ss = SparkSession.builder().master("local")
				.appName("Spark Exercise #48 - DataFrame").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: name,age,gender
		// The input file has an header. Hence, the name of the columns of
		// the defined DataFrame will be name, age, gender
		Dataset<Row> dfProfiles = ss.read().format("csv")
				.option("header", true)
				.option("inferSchema", true)
				.load(inputPath);
		
		
		// printSchema and show only for debug purposes
		// They can be removed
		dfProfiles.printSchema();
		dfProfiles.show();

		// Select only male users
		Dataset<Row> dfProfilesMale = dfProfiles.filter("gender='male'");

		// printSchema and show only for debug purposes
		// They can be removed
		dfProfilesMale.printSchema();
		dfProfilesMale.show();
		
		
		// Select only the columns name and age
		// Update also age (age = age+1)
		Dataset<Row> dfProfilesMaleIncreasedAge = dfProfilesMale
				.selectExpr("name","age+1 as age");
		
		// printSchema and show only for debug purposes
		// They can be removed
		dfProfilesMaleIncreasedAge.printSchema();
		dfProfilesMaleIncreasedAge.show();
		
		// Sort data by age (descending), name (ascending)
		Dataset<Row> dfProfilesMaleIncreasedAgeSorted = 
					dfProfilesMaleIncreasedAge.
					sort(new Column("age").desc(),new Column("name"));
		
		
		// printSchema and show only for debug purposes
		// They can be removed
		dfProfilesMaleIncreasedAgeSorted.printSchema();
		dfProfilesMaleIncreasedAgeSorted.show();
		
		dfProfilesMaleIncreasedAgeSorted.write().format("csv").option("header", true).save(outputPath);

		// Close the Spark context
		ss.stop();
	}
}
