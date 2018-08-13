package com.bonree.brfs.resourceschedule.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;

public class RandomAvailable implements AvailableServerInterface {
	private static final Logger LOG = LoggerFactory.getLogger("RandomAvailable");
	
	/**
	 * 存储资源信息
	 */
	private Map<String,ResourceModel> resourceMap = new ConcurrentHashMap<String,ResourceModel>();
	private Map<Integer, String> snIds = new ConcurrentHashMap<>();
	private LimitServerResource limit = new LimitServerResource();
	public RandomAvailable(LimitServerResource limit){
		if(limit !=null) {
			this.limit = limit;
		}
	}
	public List<ResourceModel> convertList(Map<String,ResourceModel> resourceMap,List<String> errors,int sence, LimitServerResource limit, String snName){
		List<ResourceModel> resources = new ArrayList<ResourceModel>();
		if(resourceMap.isEmpty()) {
			return resources;
		}
		String serverId = null;
		ResourceModel r = null;
		double remainValue = 0.0;
		for(Map.Entry<String, ResourceModel> entry : resourceMap.entrySet()) {
			serverId = entry.getKey();
			if(errors !=null && errors.contains(serverId)) {
				continue;
			}
			if(sence == 1) {
				remainValue = entry.getValue().getDiskRemainValue(snName);
				if(remainValue <= limit.getRemainValue()) {
					continue;
				}
			}
			r = entry.getValue();
			if(r == null) {
				continue;
			}
			resources.add(r);
		}
		return resources;
	}
	
	@Override
	public List<Pair<String, Integer>> selectAvailableServers(int scene, String storageName, List<String> exceptionServerList,int centSize)
			throws Exception {
		if(resourceMap.isEmpty()){
			return null;
		}
		List<ResourceModel> tmp = convertList(resourceMap, exceptionServerList, scene, limit, storageName);
		
		if(tmp.isEmpty()){
			return null;
		}
		
		List<Pair<String, Double>> values = new ArrayList<Pair<String, Double>>();
		if(0 == scene){
			int index = Math.abs(new Random().nextInt())%tmp.size();
		}
		if(BrStringUtils.isEmpty(storageName)){
			return null;
		}
		String server = null;
		double sum = 0.0;
		Pair<String, Double> tmpResource = null;
		for(ResourceModel ele : tmp){
			server = ele.getServerId();
			if(1 == scene){
				sum = ele.getDiskRemainValue(storageName) + ele.getDiskWriteValue(storageName);
			}else if(2 == scene){
				sum = ele.getDiskReadValue(storageName);
			}else{
				continue;
			}
			tmpResource = new Pair<String, Double>(server,sum);
			values.add(tmpResource);
		}
		if(values == null || values.isEmpty()){
			return null;
		}
		return converDoublesToIntegers(values, centSize);
	}
	@Override
	public void update(ResourceModel resource) {
		if(resource == null) {
			return;
		}
		String serverId = resource.getServerId();
		Map<Integer,String> tmpMap = resource.getSnIds();
		for(Map.Entry<Integer, String> entry : tmpMap.entrySet()) {
			this.snIds.put(entry.getKey(), entry.getValue());
		}		
		this.resourceMap.put(serverId, resource);
	}
	/**
	 * 概述：计算资源比值
	 * @param servers
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<Pair<String, Integer>> converDoublesToIntegers(final List<Pair<String, Double>> servers, int preCentSize){
		List<Pair<String,Integer>> dents = new ArrayList<Pair<String,Integer>>();
		int total = 0;
		int value = 0;
		double sum = 0;
		int centSize = preCentSize<=0 ? 100 : preCentSize;
		for(Pair<String,Double> pair: servers) {
			sum +=pair.getSecond();
		}
		Pair<String,Integer> tmp = null;
		for(Pair<String,Double> ele : servers){
			tmp = new Pair<String, Integer>();
			tmp.setFirst(ele.getFirst());
			value = (int)(ele.getSecond()/sum* centSize);
			if(value == 0){
				value = 1;
			}
			tmp.setSecond(value);
			total += value;
			dents.add(tmp);
		}
		return dents;
	}
	@Override
	public void setLimitParameter(LimitServerResource limits) {
		if(limits !=null) {
			this.limit = limits;	
		}
	}
	@Override
	public List<Pair<String, Integer>> selectAvailableServers(int scene, int snId, List<String> exceptionServerList, int centSize)
			throws Exception {
		String snName = this.snIds.get(snId);
		return selectAvailableServers(scene, snName,exceptionServerList, centSize);
	}
	@Override
	public void add(ResourceModel resource) {
		if(resource == null) {
			return;
		}
		String serverId = resource.getServerId();
		Map<Integer,String> tmpMap = resource.getSnIds();
		if(tmpMap!=null&& !tmpMap.isEmpty()) {
			this.snIds.putAll(tmpMap);
		}
		
		this.resourceMap.put(serverId, resource);
	}
	@Override
	public void remove(ResourceModel resource) {
		if(resource == null) {
			return;
		}
		String serverId = resource.getServerId();
		if(this.resourceMap.containsKey(serverId)) {
			this.resourceMap.remove(serverId);
		}		
	}
	
}
