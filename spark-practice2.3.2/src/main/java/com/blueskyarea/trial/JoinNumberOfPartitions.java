package com.blueskyarea.trial;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class JoinNumberOfPartitions {

	public static void main(String[] args) {
		new JoinNumberOfPartitions().execute();
	}
	
	private void execute() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[3]");
		sparkConf.setAppName("JoinNumberOfPartitions");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		List<AnyEntity> list = new ArrayList<>();
		IntStream.rangeClosed(1, 100).forEach(i -> {
			list.add(new AnyEntity(i, "name_" + i));
		});
		
		JavaRDD<AnyEntity> rdd = jsc.parallelize(list);
		JavaPairRDD<Integer, AnyEntity> pairRdd1 = rdd.mapToPair(r -> new Tuple2<Integer, AnyEntity>(r.getId(), r)).repartition(100);

		JavaPairRDD<Integer, AnyEntity> pairRdd2 = rdd.mapToPair(r -> new Tuple2<Integer, AnyEntity>(r.getId(), r));
				
		System.out.println(pairRdd1.partitions().size());
		System.out.println(pairRdd2.partitions().size());
		
		JavaPairRDD<Integer, Tuple2<AnyEntity, Optional<AnyEntity>>> joinedRdd = pairRdd1.leftOuterJoin(pairRdd2);
		System.out.println(joinedRdd.partitions().size());
		
		JavaPairRDD<Integer, Tuple2<AnyEntity, Optional<AnyEntity>>> joinedRdd2 = pairRdd2.leftOuterJoin(pairRdd1);
		System.out.println(joinedRdd2.partitions().size());
		
		jsc.close();
	}

}
