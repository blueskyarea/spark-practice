package com.blueskyarea;

import org.apache.spark.launcher.SparkLauncher;

public class MyLauncher {
	public static void main(String[] args) throws Exception {
		Process spark = new SparkLauncher()
				.setSparkHome("/opt/spark")
				.setAppResource("/home/mh/workspace/spark/spark-test1.6/target/spark-test1.6-1.0-SNAPSHOT.jar")
				.setMainClass("com.blueskyarea.App").setMaster("local")
				.setConf(SparkLauncher.DRIVER_MEMORY, "512m").launch();
		spark.waitFor();
		System.out.println(spark.exitValue());
	}
}
