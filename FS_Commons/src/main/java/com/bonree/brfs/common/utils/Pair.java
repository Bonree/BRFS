package com.bonree.brfs.common.utils;

public class Pair<T1, T2> {
	private T1 key;
	private T2 value;
	public Pair(T1 key, T2 value){
		this.key = key;
		this.value = value;
	}
	public Pair(){
		
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
	public String toString(){
		return this.key + ":" + this.value;
	}
	public boolean isBothEmpty(){
		return isEmptyKey() && isEmptyValue();
	}
	public boolean isEmptyKey(){
		return this.key == null;
	}
	public boolean isEmptyValue(){
		return this.value == null;
	}
	
}
