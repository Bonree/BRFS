package com.bonree.brfs.resourceschedule.model;

public class ResourcePair<T1, T2> {
	private T1 key;
	private T2 value;
	public ResourcePair(T1 key, T2 value){
		this.key = key;
		this.value = value;
	}
	public ResourcePair(){
		
	}
	public T1 getKey() {
		return key;
	}
	public void setKey(T1 key) {
		this.key = key;
	}
	public T2 getValue() {
		return value;
	}
	public void setValue(T2 value) {
		this.value = value;
	}
	
}
