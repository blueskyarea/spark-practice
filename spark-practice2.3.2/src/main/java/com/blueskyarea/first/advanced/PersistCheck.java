package com.blueskyarea.first.advanced;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class PersistCheck {
	private static final Logger logger = Logger.getLogger(PersistCheck.class);
	
	public static void main(String[] args) {
		new PersistCheck().start();
	}

	private void start() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[*]");
		sparkConf.setAppName("PersistCheck");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		List<Integer> list = new ArrayList<Integer>(){
			{
				add(1);
				add(2);
				add(3);
			}
		};
		
		JavaRDD<Integer> rdd = jsc.parallelize(list);
		JavaRDD<Integer> rdd2 = rdd.map(x -> x * 5);
		//JavaRDD<Integer> rdd2 = rdd.map(x -> x * 5).persist(StorageLevel.MEMORY_ONLY());
		//rdd2.persist(StorageLevel.MEMORY_ONLY());
		
		logger.info("start to read rdd 1st");
		rdd2.foreach(System.out::println);
		logger.info("start to read rdd 2nd");
		rdd2.foreach(System.out::println);
		
		jsc.close();
	}
}
