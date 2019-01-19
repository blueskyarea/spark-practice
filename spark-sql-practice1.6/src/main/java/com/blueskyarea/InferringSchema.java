package com.blueskyarea;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferringSchema {
	Logger logger = LoggerFactory.getLogger(InferringSchema.class);

	public static void main(String[] args) {
		new InferringSchema().execute();
	}

	private void execute() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[3]");
		sparkConf.setAppName("InferringSchema");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
		
		// read json as JavaRDD
		JavaRDD<Person> personRdd = sqlContext.read().json("src/main/resources/people.json").toJavaRDD().map(data -> {
			Person person = new Person();
			person.setAge((int)data.getLong(0));
			person.setName(data.getString(1));
			return person;
		});
		
		DataFrame schemaPeople = sqlContext.createDataFrame(personRdd, Person.class);
		schemaPeople.registerTempTable("people");
		
		logger.info("show teenagers");
		DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");
		teenagers.show();
		
		logger.info("toJavaRDD");
		teenagers.toJavaRDD().foreach(row -> System.out.println(row.getString(0)));
	}

	public static class Person implements Serializable {
		private static final long serialVersionUID = 276596298931996443L;
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}
}
