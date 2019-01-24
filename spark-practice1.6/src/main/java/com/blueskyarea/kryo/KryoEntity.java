package com.blueskyarea.kryo;

import java.io.Serializable;

public class KryoEntity {
	private int index;
	private String name;
	
	public KryoEntity(int index, String name) {
		this.index = index;
		this.name = name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}
