package com.blueskyarea.first.advanced;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

public class AccumulatorV2Try implements Serializable {

	public static void main(String[] args) {
		new AccumulatorV2Try().execute();
	}

	private void execute() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[3]");
		sparkConf.setAppName("Accumulater");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd = jsc.textFile("src/main/resources/file1.txt");
		
		final AccumulatorV2<Integer, Integer> blankLines = new MyAccumulator(0);
		jsc.sc().register(blankLines);
		rdd.foreach(line -> {
			if (line.equals("")) {
				blankLines.add(1);
			}
		});
		
		System.out.println(blankLines.value());
		jsc.close();
	}
	
	class MyAccumulator extends AccumulatorV2<Integer, Integer> {
		
		private int count = 0;
		public MyAccumulator(int initialValue) {
			this.count = initialValue;
		}
		
		@Override
		public void add(Integer arg0) {
			this.count += arg0;
		}
		
		@Override
		public AccumulatorV2<Integer, Integer> copy() {
			return new MyAccumulator(value());
		}
		
		@Override
		public boolean isZero() {
			if (this.count == 0) {
				return true;
			}
			return false;
		}
		
		@Override
		public void merge(AccumulatorV2<Integer, Integer> arg0) {
			add(arg0.value());
		}
		
		@Override
		public void reset() {
			this.count = 0;
		}
		
		@Override
		public Integer value() {
			return this.count;
		}
	}
}
