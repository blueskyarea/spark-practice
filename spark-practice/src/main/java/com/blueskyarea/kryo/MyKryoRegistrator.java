package com.blueskyarea.kryo;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class MyKryoRegistrator implements KryoRegistrator {

	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register(KryoEntity.class);
	}
	
}
