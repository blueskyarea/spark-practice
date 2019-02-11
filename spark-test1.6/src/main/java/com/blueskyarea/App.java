package com.blueskyarea;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	Logger logger = LoggerFactory.getLogger(App.class);
	
    public static void main( String[] args ) {
        new App().execute();
    }
    
    protected void execute() {
    	SparkConf sparkConf = new SparkConf();
    	sparkConf.setAppName("App");
    	JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    	
    	List<String> list = new ArrayList<>();
    	IntStream.rangeClosed(1, 10000).forEach(i -> {
    		list.add("data:" + i);
    	});
    	
    	JavaRDD<String> rdd = jsc.parallelize(list).persist(StorageLevel.MEMORY_AND_DISK());
    	logger.info("First count:" + rdd.count());
    	
    	JavaRDD<String> finalRdd = jsc.emptyRDD();
    	for (int i = 0; i < 10; i++) {
    		
    	}
    	for (int i = 0; i < 10; i++) {
    		logger.info("repartition:" + i);
    		JavaRDD<String> rdd2 = rdd.filter(d -> d.contains("5"));
    		finalRdd = rdd2.repartition(1000);
    		try {
				Thread.sleep(2000);
			} catch (Exception e) {
				// nothing to do
			}
    	}
    	
    	logger.info("Final count:" + finalRdd.count());
    	jsc.close();
    }
}
