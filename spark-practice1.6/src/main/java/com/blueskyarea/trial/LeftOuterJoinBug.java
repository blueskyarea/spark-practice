package com.blueskyarea.trial;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LeftOuterJoinBug {
	public static void main(String[] args) {
		new LeftOuterJoinBug().execute();
	}
	
	private void execute() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[3]");
		sparkConf.setAppName("LeftOuterJoinBug");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		List<AnyEntity> list = new ArrayList<>();
		IntStream.rangeClosed(1, 100).forEach(i -> {
			list.add(new AnyEntity(i, "name_" + i));
		});
		
		JavaRDD<AnyEntity> rdd = jsc.parallelize(list);
		JavaPairRDD<Integer, AnyEntity> pairRdd1 = rdd.mapToPair(r -> new Tuple2<Integer, AnyEntity>(r.getId(), r));
		JavaPairRDD<Integer, AnyEntity> pairRdd2 = rdd.mapToPair(r -> new Tuple2<Integer, AnyEntity>(r.getId(), null));
		
		JavaPairRDD<Integer, Tuple2<AnyEntity, com.google.common.base.Optional<AnyEntity>>> joinedRdd = pairRdd1.leftOuterJoin(pairRdd2);
		joinedRdd.count();
		
		jsc.close();
	}
}
