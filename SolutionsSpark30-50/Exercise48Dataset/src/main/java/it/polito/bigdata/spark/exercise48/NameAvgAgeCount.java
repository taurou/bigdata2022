package it.polito.bigdata.spark.exercise48;

import java.io.Serializable;

@SuppressWarnings("serial")
public class NameAvgAgeCount implements Serializable {
	private String name;
	private double avgage;
	private long count;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public double getAvgage() {
		return avgage;
	}
	public void setAvgage(double avgage) {
		this.avgage = avgage;
	}

}
