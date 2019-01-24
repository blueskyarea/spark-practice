package com.blueskyarea.kryo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class KryoMain {
	
	private static final int listSize = 10000;
	private static final StopWatch stopWatch = new StopWatch();
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[3]");
		sparkConf.setAppName("KryoMain");
	    sparkConf.set("spark.kryo.registrator", "com.blueskyarea.kryo.MyKryoRegistrator");
	    sparkConf.set("spark.kryoserializer.buffer", "64m");
	    sparkConf.set("spark.kryoserializer.buffer.max", "128m");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		try {
			// Kryo Serialization
			List<KryoEntity> kryoList2 = new ArrayList<>();
			for (int i = 0; i < listSize; i++) {
				kryoList2.add(new KryoEntity(i, i + ":name"));
			}
			
			JavaRDD<KryoEntity> kryoRdd2 = jsc.parallelize(kryoList2);
			JavaRDD<KryoEntity> convertedRdd2 = new KryoConverter().convert(kryoRdd2);
			stopWatch.start();
			System.out.println(convertedRdd2.count());
			stopWatch.stop();
			System.out.println("In case of Kryo Serialization " + stopWatch);
			stopWatch.reset();
			
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
			
			// Java Serialization
			List<JavaSerializeEntity> jseList2 = new ArrayList<>();
			for (int i = 0; i < listSize; i++) {
				jseList2.add(new JavaSerializeEntity(i, i + ":name"));
			}
			
			JavaRDD<JavaSerializeEntity> jseRdd2 = jsc.parallelize(jseList2);
			stopWatch.start();
			System.out.println(jseRdd2.count());
			stopWatch.stop();
			System.out.println("In case of Java Serialization " + stopWatch);
			stopWatch.reset();
			

	
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jsc.close();
		}
	}

}
