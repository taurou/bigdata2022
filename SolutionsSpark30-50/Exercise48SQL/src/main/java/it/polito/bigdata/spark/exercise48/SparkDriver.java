package it.polito.bigdata.spark.exercise48;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	public static void main(String[] args) {
		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF); 

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #48 - SQL").getOrCreate();
		//SparkSession ss = SparkSession.builder().master("local").appName("Spark Exercise #48 - DataFrame").getOrCreate();

		// Read the content of the input file and store it into a DataFrame
		// Meaning of the columns of the input file: name,age,gender
		// The input file has an header. Hence, the name of the columns of
		// the defined DataFrame will be name, age, gender
		Dataset<Row> dfProfiles = ss.read().format("csv")
				.option("header", true).option("inferSchema", true)
				.load(inputPath);

		// Define a Dataset of Profile objects from the dfReading DataFrame
		Dataset<Profile> dsProfiles = dfProfiles.as(Encoders.bean(Profile.class));
		
		// printSchema and show only for debug purposes
		// They can be removed
		dsProfiles.printSchema();
		dsProfiles.show();

		
		// Assign the “table name” profiles the dsProfiles Dataset	
		dsProfiles.createOrReplaceTempView("profiles");
		
		// Write an SQL-like query to select only name and age+1 of male users
		Dataset<NameAvgAge> dsSeletedNames = 
				ss.sql("SELECT name, avg(age) as avgage "
						+ "FROM profiles "
						+ "GROUP BY name "
						+ "HAVING count(*)>=2")
				.as(Encoders.bean(NameAvgAge.class)); 
		
		// printSchema and show only for debug purposes
		// They can be removed
		dsSeletedNames.printSchema();
		dsSeletedNames.show(); 
		
		dsSeletedNames.write().format("csv").option("header", false).save(outputPath);

		// Close the Spark context
		ss.stop();
		

	}
}
