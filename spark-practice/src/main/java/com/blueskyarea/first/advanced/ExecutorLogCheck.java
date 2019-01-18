package com.blueskyarea.first.advanced;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ExecutorLogCheck {
	private static final Logger logger = Logger.getLogger(ExecutorLogCheck.class);

	public static void main(String[] args) throws InterruptedException {
		new ExecutorLogCheck().start();
	}

	private void start() throws InterruptedException {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[*]");
		sparkConf.setAppName("DriverLogOrder");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		List<Integer> list = new ArrayList<Integer>(){
			{
				add(1);
				add(2);
				add(3);
			}
		};
		
		JavaRDD<Integer> rdd = jsc.parallelize(list);
		JavaRDD<Integer> rdd2 = rdd.map(x -> {
			logger.info("converting " + x);
			return x * 5;
		});
		
		rdd2.foreach(System.out::println);
		Thread.sleep(60000);
		jsc.close();
	}
}
