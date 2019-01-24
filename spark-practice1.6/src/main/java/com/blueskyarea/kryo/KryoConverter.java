package com.blueskyarea.kryo;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

public class KryoConverter {
	public KryoConverter() {
	}
	
	public JavaRDD<KryoEntity> convert(JavaRDD<KryoEntity> rdd) {
		return rdd.map(r -> {
			//r.setName(createName());
			r.setName("dummyName");
			return r;
		});
	}
	
	private String createName() {
		return "dummyName";
	}
}
