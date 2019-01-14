package com.blueskyarea.kryo;

import java.io.Serializable;

public class JavaSerializeEntity implements Serializable {

	private static final long serialVersionUID = 4756588323193705449L;
	private int index;
	private String name;
	
	public JavaSerializeEntity(int index, String name) {
		this.index = index;
		this.name = name;
	}
}
