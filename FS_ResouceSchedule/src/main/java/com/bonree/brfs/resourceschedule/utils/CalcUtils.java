package com.bonree.brfs.resourceschedule.utils;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.common.utils.BrStringUtils;

public class CalcUtils{
	public static  Map<String, Long> diffDataMap(Map<String, Long> map1, Map<String,Long> map2){
		Map<String,Long> map = new ConcurrentHashMap<String,Long>();
		if(map1 == null || map1.isEmpty()){
			return map;
		}
		if(map2 == null || map2.isEmpty()){
			map.putAll(map1);
			return map;
		}
		Set<String> keys = new HashSet<String>();
		keys.addAll(map1.keySet());
		keys.addAll(map2.keySet());
		for(String key : keys){
			if(!map1.containsKey(key)){
				continue;
			}
			if(!map2.containsKey(key)){
				map.put(key, map1.get(key));
			}else{
				map.put(key, map1.get(key) - map2.get(key));
			}
		}
		return map;
	}
	public static Map<String, Long> sumDataMap(Map<String, Long> map1, Map<String,Long> map2){
		Map<String,Long> map = new ConcurrentHashMap<String,Long>();
		if((map1 == null || map1.isEmpty()) && (map2 == null || map2.isEmpty())){
			return map;
		}else if(map2 == null || map2.isEmpty()){
			map.putAll(map1);
			return map;
		}else if(map1 == null || map1.isEmpty()){
			map.putAll(map2);
			return map;
		}
		Set<String> keys = new HashSet<String>();
		keys.addAll(map1.keySet());
		keys.addAll(map2.keySet());
		for(String key : keys){
			if(!map1.containsKey(key)){
				map.put(key, map2.get(key));
			}else if(!map2.containsKey(key)){
				map.put(key, map1.get(key));
			}else{
				map.put(key, map1.get(key) + map2.get(key));
			}
		}
		return map;
	}
	public static Map<String, Long> divDataMap(Map<String,Long> map1, long count){
		Map<String,Long> map = new ConcurrentHashMap<String,Long>();
		if(map1 == null || map1.isEmpty() ||count == 0){
			return map;
		}else if(count == 1){
			map.putAll(map1);
			return map;
		}
		String key = null;
		long value = 0l;
		for(Map.Entry<String, Long> entry : map1.entrySet()){
			key = entry.getKey();
			value = entry.getValue();
			if(BrStringUtils.isEmpty(key)){
				continue;
			}
			if(!map.containsKey(key)){
				map.put(key, value/count);
			}
		}
		return map;
	}
	
	public static Map<String, Double> divDataDoubleMap(Map<String,Long> map1, long count){
		Map<String,Double> map = new ConcurrentHashMap<String,Double>();
		if(map1 == null || map1.isEmpty() ||count == 0){
			return map;
		}
		String key = null;
		long value = 0l;
		double result = 0.0;
		for(Map.Entry<String, Long> entry : map1.entrySet()){
			key = entry.getKey();
			value = entry.getValue();
			if(BrStringUtils.isEmpty(key)){
				continue;
			}
			if(!map.containsKey(key)){
				result = (double)value/count;
				map.put(key, result);
			}
		}
		return map;
	}
	public static Map<String, Double> divDiffDataDoubleMap(Map<String,Long> map1, long count){
		Map<String,Double> map = new ConcurrentHashMap<String,Double>();
		if(map1 == null || map1.isEmpty() ||count == 0){
			return map;
		}
		String key = null;
		long value = 0l;
		double result = 0.0;
		for(Map.Entry<String, Long> entry : map1.entrySet()){
			key = entry.getKey();
			value = entry.getValue();
			if(BrStringUtils.isEmpty(key)){
				continue;
			}
			if(!map.containsKey(key)){
				result = (double)(count - value)/count;
				map.put(key, result);
			}
		}
		return map;
	}
	
	public static long collectDataMap(Map<String,Long> map1){
		if(map1 == null || map1.isEmpty()){
			return 0;
		}
		long value = 0l;
		for(Map.Entry<String, Long> entry : map1.entrySet()){
			value += entry.getValue();
		}
		return value;
	}
	
	public static long maxDataMap(Map<String,Long> map1){
		if(map1 == null || map1.isEmpty()){
			return 0;
		}
		long value = 0l;
		for(long tmp : map1.values()){
			if(value < tmp){
				value = tmp;
			}
		}
		return value;
	}
}
