package com.bonree.brfs.resourceschedule.commons.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.resourceschedule.commons.CommonMapCalcInterface;
import com.bonree.brfs.resourceschedule.commons.ModelCalcInterface;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月22日 下午5:32:54
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 数据汇总接口
 *****************************************************************************
 */
public class MapModelCalc<T1,T2 extends ModelCalcInterface<T2>> implements CommonMapCalcInterface<T1, T2> {

	@Override
	public Map<T1, T2> calcMapData(Map<T1, T2> minuendMap, Map<T1, T2> subtrahendMap) {
		Map<T1,T2> differenceMap = new ConcurrentHashMap<T1,T2>();
		if(minuendMap == null){
			differenceMap.putAll(subtrahendMap);
			return differenceMap;
		}
		if(subtrahendMap == null){
			differenceMap.putAll(minuendMap);
			return differenceMap;
		}
		Set<T1> keys = new HashSet<T1>();
		keys.addAll(minuendMap.keySet());
		keys.addAll(subtrahendMap.keySet());
		T2 minuendObj = null;
		T2 subtrahendObj = null;
		for(T1 key : keys){
			if(key == null){
				continue;
			}
			
			if((!minuendMap.containsKey(key) && !subtrahendMap.containsKey(key))
					|| !minuendMap.containsKey(key)
					|| (minuendMap.containsKey(key) && minuendMap.get(key) == null)){
				continue;
			}
			if(minuendMap.containsKey(key) && subtrahendMap.containsKey(key)
					&& subtrahendMap.get(key) != null){
				minuendObj = minuendMap.get(key);
				subtrahendObj = subtrahendMap.get(key);
				differenceMap.put(key, minuendObj.calc(subtrahendObj));
			}else {
				minuendObj = minuendMap.get(key);
				differenceMap.put(key, minuendObj);
			}
		}
		return differenceMap;
	}

	@Override
	public Map<T1, List<T2>> collectModels(Map<T1, List<T2>> collectMap, Map<T1, T2> sourceMap) {
		Map<T1,List<T2>> objMap = new HashMap<T1,List<T2>>();
		if(collectMap !=null && !collectMap.isEmpty()){
			objMap.putAll(collectMap);
		}
		T1 key = null;
		T2 value = null;
		for(Map.Entry<T1, T2> entry : sourceMap.entrySet()){
			value = entry.getValue();
			key = entry.getKey();
			if(value == null || key == null){
				continue;
			}
			if(!objMap.containsKey(key)){
				objMap.put(key, new ArrayList<T2>());
			}
			objMap.get(key).add(value);
		}
		return objMap;
	}

	@Override
	public T2 sumList(List<T2> collect) {
		T2 sumObj = null;
		if(collect == null || collect.isEmpty()){
			return null;
		}
		for(T2 tmp : collect){
			sumObj = sumObj.sum(tmp);
		}
		return sumObj;
	}

	@Override
	public Map<T1, T2> sumMapData(Map<T1, List<T2>> collect) {
		Map<T1, T2> sumMap = new HashMap<T1,T2>();
		if(collect == null){
			return sumMap;
		}
		T1 key = null;
		List<T2> value = null;
		T2 result = null;
		for(Entry<T1, List<T2>> entry : collect.entrySet()){
			key = entry.getKey();
			value = entry.getValue();
			if(key == null){
				continue;
			}
			if(value == null || value.isEmpty()){
				continue;
			}
			result = sumList(value);
			if(result == null){
				continue;
			}
			if(!sumMap.containsKey(key)){
				sumMap.put(key, result);
			}
		}
		return sumMap;
	}

}
