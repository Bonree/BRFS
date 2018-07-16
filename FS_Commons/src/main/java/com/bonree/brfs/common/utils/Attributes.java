package com.bonree.brfs.common.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public final class Attributes {
	private Map<String, Object> attributes = new HashMap<String, Object>();
	
	public Attributes() {
	}
	
	public Attributes(Map<String, Object> map) {
		for(Entry<String,Object> entry : map.entrySet()) {
			attributes.put(entry.getKey(), entry.getValue());
		}
	}
	
	public Attributes(Attributes other) {
		this(other.attributes);
	}
	
	public boolean isEmpty() {
		return attributes.isEmpty();
	}
	
	public Set<String> getAttributeNames() {
		return attributes.keySet();
	}
	
	public void putBoolean(String name, boolean value) {
		attributes.put(name, value);
	}
	
	public void putByte(String name, byte value) {
		attributes.put(name, value);
	}
	
	public void putShort(String name, short value) {
		attributes.put(name, value);
	}
	
	public void putChar(String name, char value) {
		attributes.put(name, value);
	}
	
	public void putInt(String name, int value) {
		attributes.put(name, value);
	}
	
	public void putLong(String name, long value) {
		attributes.put(name, value);
	}
	
	public void putFloat(String name, float value) {
		attributes.put(name, value);
	}
	
	public void putDouble(String name, double value) {
		attributes.put(name, value);
	}
	
	public void putString(String name, String value) {
		attributes.put(name, value);
	}
	
	public void putObject(String name, Object value) {
		attributes.put(name, value);
	}
	
	public boolean getBoolean(String name) {
		return (boolean) attributes.get(name);
	}
	
	public boolean getBoolean(String name, boolean defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : (boolean) object;
	}
	
	public byte getByte(String name) {
		return (byte) attributes.get(name);
	}
	
	public byte getByte(String name, byte defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : (byte) object;
	}
	
	public short getShort(String name) {
		return (short) attributes.get(name);
	}
	
	public short getShort(String name, short defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : (short) object;
	}
	
	public char getChar(String name) {
		return (char) attributes.get(name);
	}
	
	public char getChar(String name, char defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : (char) object;
	}
	
	public int getInt(String name) {
		return (int) attributes.get(name);
	}
	
	public int getInt(String name, int defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : (int) object;
	}
	
	public long getLong(String name) {
		return (long) attributes.get(name);
	}
	
	public long getLong(String name, long defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : (long) object;
	}
	
	public float getFloat(String name) {
		return (float) attributes.get(name);
	}
	
	public float getFloat(String name, float defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : (float) object;
	}
	
	public double getDouble(String name) {
		return (double) attributes.get(name);
	}
	
	public double getDouble(String name, double defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : (double) object;
	}
	
	public String getString(String name) {
		return (String) attributes.get(name);
	}
	
	public String getString(String name, String defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : (String) object;
	}
	
	public Object getObject(String name) {
		return attributes.get(name);
	}
	
	public Object getObject(String name, Object defaultValue) {
		Object object =  attributes.get(name);
		
		return object == null ? defaultValue : object;
	}
}
