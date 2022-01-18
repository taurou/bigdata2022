package it.polito.bigdata.hadoop.exercise17;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper second data format
 */
class MapperType2BigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		FloatWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String record = value.toString();
		// Split each record by using the field separator
		// fields[0]= date
		// fields[1]= hour:minute
		// fields[2]= temperature
		// fields[3]= sensor id
		String[] fields = record.split(",");

		String date = fields[0];
		float temperature = Float.parseFloat(fields[2]);

		context.write(new Text(date), new FloatWritable(temperature));
	}

}
