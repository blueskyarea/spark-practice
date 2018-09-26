package com.blueskyarea.first.advanced;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastTry {
	private StopWatch stopWatch = new StopWatch();

	public static void main(String[] args) {
		new BroadcastTry().execute();
	}

	private void execute() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[3]");
		sparkConf.setAppName("BroadcastTry");
		try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
			List<Integer> bigList = new ArrayList<>();
			IntStream.rangeClosed(1, 1000000).forEach(i -> bigList.add(i));

			JavaRDD<String> nameRdd = jsc.parallelize(Arrays.asList("A", "B",
					"C"));
			withoutBroadcast(nameRdd, bigList);

			JavaRDD<String> nameRdd2 = jsc.parallelize(Arrays.asList("D", "E",
					"F"));
			Broadcast<List<Integer>> broadcast = jsc.broadcast(bigList);
			withBroadcast(nameRdd2, broadcast);
		}
	}

	private void withoutBroadcast(JavaRDD<String> nameRdd, List<Integer> bigList) {
		stopWatch.start();
		JavaRDD<List<String>> nameWithIndex = nameRdd.map(name -> {
			return bigList.stream().map(i -> {
				return name.concat(String.valueOf(i));
			}).collect(Collectors.toList());
		});

		System.out.println(nameWithIndex.count());
		stopWatch.stop();
		System.out.println(stopWatch);
		stopWatch.reset();
	}

	private void withBroadcast(JavaRDD<String> nameRdd,
			Broadcast<List<Integer>> broadcast) {
		stopWatch.start();
		JavaRDD<List<String>> nameWithIndex = nameRdd.map(name -> {
			return broadcast.getValue().stream().map(i -> {
				return name.concat(String.valueOf(i));
			}).collect(Collectors.toList());
		});

		System.out.println(nameWithIndex.count());
		stopWatch.stop();
		System.out.println(stopWatch);
		stopWatch.reset();
	}
}
