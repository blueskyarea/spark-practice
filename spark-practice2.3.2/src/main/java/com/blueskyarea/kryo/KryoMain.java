package com.blueskyarea.kryo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class KryoMain {
	
	private static final int listSize = 100;
	private static final StopWatch stopWatch = new StopWatch();
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[3]");
		sparkConf.setAppName("KryoMain");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	    sparkConf.set("spark.kryo.registrator", "com.blueskyarea.kryo.MyKryoRegistrator");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		try {
			// Java Serialization
			List<JavaSerializeEntity> jseList = new ArrayList<>();
			for (int i = 0; i < listSize; i++) {
				jseList.add(new JavaSerializeEntity(i, i + ":name"));
			}
			
			JavaRDD<JavaSerializeEntity> jseRdd = jsc.parallelize(jseList);
			stopWatch.start();
			System.out.println(jseRdd.count());
			stopWatch.stop();
			System.out.println("In case of Java Serialization " + stopWatch);
			stopWatch.reset();
			
			// Kryo Serialization
			List<KryoEntity> kryoList = new ArrayList<>();
			for (int i = 0; i < listSize; i++) {
				kryoList.add(new KryoEntity(i, i + ":name"));
			}
			
			JavaRDD<KryoEntity> kryoRdd = jsc.parallelize(kryoList);
			stopWatch.start();
			System.out.println(kryoRdd.count());
			stopWatch.stop();
			System.out.println("In case of Kryo Serialization " + stopWatch);
			stopWatch.reset();
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jsc.close();
		}
	}

}
