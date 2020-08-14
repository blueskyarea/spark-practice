package com.blueskyarea.first.advanced;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AccumulatorTry {

	public static void main(String[] args) {
		new AccumulatorTry().execute();
	}

	private void execute() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[3]");
		sparkConf.setAppName("Accumulater");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd = jsc.textFile("src/main/resources/file1.txt");
		
		final Accumulator<Integer> blankLines = jsc.accumulator(0);
		rdd.foreach(line -> {
			if (line.equals("")) {
				blankLines.add(1);
			}
		});
		
		System.out.println(blankLines.value());
		jsc.close();
	}
}
