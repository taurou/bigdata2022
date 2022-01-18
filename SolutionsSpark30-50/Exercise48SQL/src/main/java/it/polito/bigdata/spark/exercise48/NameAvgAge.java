package it.polito.bigdata.spark.exercise48;

import java.io.Serializable;

@SuppressWarnings("serial")
public class NameAvgAge implements Serializable {
	private String name;
	private double avgage;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getAvgage() {
		return avgage;
	}

	public void setAvgage(double avgage) {
		this.avgage = avgage;
	}

}
