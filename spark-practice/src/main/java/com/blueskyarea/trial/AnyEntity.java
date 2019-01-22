package com.blueskyarea.trial;

import java.io.Serializable;

public class AnyEntity implements Serializable {
	private int id;
	private String name;
	
	public AnyEntity(int id, String name) {
		this.id = id;
		this.name = name;
	}
	
	public int getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
}
