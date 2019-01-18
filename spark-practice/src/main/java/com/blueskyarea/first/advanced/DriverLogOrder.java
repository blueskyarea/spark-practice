package com.blueskyarea.first.advanced;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DriverLogOrder {
	private static final Logger logger = Logger.getLogger(DriverLogOrder.class);

	public static void main(String[] args) {
		new DriverLogOrder().start();
	}
	
	private void start() {
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
		
		logger.info("1.start to create rdd.");
		JavaRDD<Integer> rdd = jsc.parallelize(list);
		logger.info("1.end to create rdd.");
		
		logger.info("2.start to convert rdd.");
		JavaRDD<Integer> rdd2 = rdd.map(x -> x * 5);
		logger.info("2.end to convert rdd.");
		
		logger.info("3.start to read rdd.");
		rdd2.foreach(System.out::println);
		logger.info("3.end to read rdd.");
		
		jsc.close();
	}

}
