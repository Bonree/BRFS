package com.bonree.brfs.resourceschedule.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collection;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.ResourcePair;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;

public class RandomAvailable implements AvailableServerInterface {
	/**
	 * 存储资源信息
	 */
	private List<ResourceModel> resource = new ArrayList<ResourceModel>();
	private long updateTime = 0;
	private RandomAvailable(){
		
	}
	private static class simpleInstance{
		public static  RandomAvailable  instance = new RandomAvailable();
	}
	public static RandomAvailable getInstance(){
		return simpleInstance.instance;
	}
	/**
	 * scene场景 0：元数据操作，1：写入操作，2：读取操作
	 */
	@Override
	public String selectAvailableServer(int scene, String storageName) throws Exception {
		
		if(this.resource.isEmpty()){
			return null;
		}
		
		List<ResourcePair<String, Double>> values = new ArrayList<ResourcePair<String, Double>>();
		if(0 == scene){
			int index = Math.abs(new Random().nextInt())%this.resource.size();
			return this.resource.get(index).getServerId();
		}
		if(BrStringUtils.isEmpty(storageName)){
			return null;
		}
		String server = null;
		double sum = 0.0;
		ResourcePair<String, Double> tmp = null;
		for(ResourceModel ele : resource){
			server = ele.getServerId();
			if(1 == scene){
				sum = ele.getDiskRemainRate() + ele.getDiskWriteValue(storageName);
			}else if(2 == scene){
				sum = ele.getDiskReadValue(storageName);
			}else{
				continue;
			}
			tmp = new ResourcePair<String, Double>();
			tmp.setKey(server);
			tmp.setValue(sum);
		}
		if(values.isEmpty()){
			return null;
		}
		int index = getWeightRandom(values);
		
		return values.get(index).getKey();
	}
	@Override
	public String selectAvailableServer(int scene, String storageName, List<String> exceptionServerList)
			throws Exception {
		if(this.resource.isEmpty()){
			return null;
		}
		List<ResourceModel> tmp = new ArrayList<ResourceModel>();
		if(exceptionServerList !=null && !exceptionServerList.isEmpty()){
			for(ResourceModel ele : this.resource){
				if(exceptionServerList.contains(ele.getServerId())){
					continue;
				}
				tmp.add(ele);
			}
			
		}else{
			tmp.addAll(this.resource);
		}
		if(tmp.isEmpty()){
			return null;
		}
		List<ResourcePair<String, Double>> values = new ArrayList<ResourcePair<String, Double>>();
		if(0 == scene){
			int index = Math.abs(new Random().nextInt())%tmp.size();
		}
		if(BrStringUtils.isEmpty(storageName)){
			return null;
		}
		String server = null;
		double sum = 0.0;
		ResourcePair<String, Double> tmpResource = null;
		for(ResourceModel ele : tmp){
			server = ele.getServerId();
			if(exceptionServerList !=null && exceptionServerList.contains(server)){
				continue;
			}
			if(1 == scene){
				sum = ele.getDiskRemainRate() + ele.getDiskWriteValue(storageName);
			}else if(2 == scene){
				sum = ele.getDiskReadValue(storageName);
			}else{
				continue;
			}
			tmpResource = new ResourcePair<String, Double>();
			tmpResource.setKey(server);
			tmpResource.setValue(sum);
			values.add(tmpResource);
		}
		if(values.isEmpty()){
			return null;
		}
		int index = getWeightRandom(values);
		return values.get(index).getKey();
	}
	@Override
	public List<ResourcePair<String, Integer>> selectAvailableServers(int scene, String storageName) throws Exception {
		if(this.resource.isEmpty()){
			return null;
		}
		List<ResourceModel> tmp = new ArrayList<ResourceModel>();
		tmp.addAll(this.resource);
		if(tmp.isEmpty()){
			return null;
		}
		List<ResourcePair<String, Double>> values = new ArrayList<ResourcePair<String, Double>>();
		if(0 == scene){
			int index = Math.abs(new Random().nextInt())%tmp.size();
		}
		if(BrStringUtils.isEmpty(storageName)){
			return null;
		}
		String server = null;
		double sum = 0.0;
		ResourcePair<String, Double> tmpResource = null;
		for(ResourceModel ele : tmp){
			server = ele.getServerId();
			if(1 == scene){
				sum = ele.getDiskRemainRate() + ele.getDiskWriteValue(storageName);
			}else if(2 == scene){
				sum = ele.getDiskReadValue(storageName);
			}else{
				continue;
			}
			tmpResource = new ResourcePair<String, Double>();
			tmpResource.setKey(server);
			tmpResource.setValue(sum);
			values.add(tmpResource);
		}
		return converDoublesToIntegers(values);
	}
	@Override
	public List<ResourcePair<String, Integer>> selectAvailableServers(int scene, String storageName, List<String> exceptionServerList)
			throws Exception {
		if(this.resource.isEmpty()){
			return null;
		}
		List<ResourceModel> tmp = new ArrayList<ResourceModel>();
		if(exceptionServerList !=null && !exceptionServerList.isEmpty()){
			for(ResourceModel ele : this.resource){
				if(exceptionServerList.contains(ele.getServerId())){
					continue;
				}
				tmp.add(ele);
			}
			
		}else{
			tmp.addAll(this.resource);
		}
		if(tmp.isEmpty()){
			return null;
		}
		List<ResourcePair<String, Double>> values = new ArrayList<ResourcePair<String, Double>>();
		if(0 == scene){
			int index = Math.abs(new Random().nextInt())%tmp.size();
		}
		if(BrStringUtils.isEmpty(storageName)){
			return null;
		}
		String server = null;
		double sum = 0.0;
		ResourcePair<String, Double> tmpResource = null;
		for(ResourceModel ele : tmp){
			server = ele.getServerId();
			if(exceptionServerList !=null && exceptionServerList.contains(server)){
				continue;
			}
			if(1 == scene){
				sum = ele.getDiskRemainRate() + ele.getDiskWriteValue(storageName);
			}else if(2 == scene){
				sum = ele.getDiskReadValue(storageName);
			}else{
				continue;
			}
			tmpResource = new ResourcePair<String, Double>();
			tmpResource.setKey(server);
			tmpResource.setValue(sum);
			values.add(tmpResource);
		}
		return converDoublesToIntegers(values);
		
	}
	@Override
	public void update(Collection<ResourceModel> resources) {
		if(resources == null || resources.isEmpty()){
			return;
		}
		this.resource.clear();
		this.resource.addAll(resources);
		this.updateTime = System.currentTimeMillis();
	}
	@Override
	public long getLastUpdateTime() {
		return updateTime;
	}
	/**
	 * 概述：权重随机数
	 * @param servers
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private int getWeightRandom(List<ResourcePair<String, Double>> servers){
		List<ResourcePair<String,Integer>> dents = new ArrayList<ResourcePair<String,Integer>>();
		int total = 0;
		ResourcePair<String,Integer> tmp = null;
		for(ResourcePair<String,Double> ele : servers){
			tmp = converDoubleToInteger(ele, 50);
			dents.add(tmp);
			total += tmp.getValue();
		}
		Random random = new Random();
		int randomNum = Math.abs(random.nextInt()%total);
		int current = 0;
		for(ResourcePair<String, Integer> ele : dents){
			current += ele.getValue();
			if(randomNum > current){
				current ++;
				continue;
			}
			if(randomNum <=current){
				break;
			}
		}
		return current;
	}
	private List<ResourcePair<String, Integer>> converDoublesToIntegers(List<ResourcePair<String, Double>> servers){
		List<ResourcePair<String,Integer>> dents = new ArrayList<ResourcePair<String,Integer>>();
		int total = 0;
		ResourcePair<String,Integer> tmp = null;
		for(ResourcePair<String,Double> ele : servers){
			tmp = converDoubleToInteger(ele, 50);
			dents.add(tmp);
			total += tmp.getValue();
		}
		return dents;
	}
	private ResourcePair<String,Integer> converDoubleToInteger(ResourcePair<String, Double> source, int baseLine){
		ResourcePair<String,Integer> dent = new ResourcePair<String, Integer>();
		dent.setKey(source.getKey());
		dent.setValue((int)(source.getValue() * baseLine));
		return dent;
	}
	@Override
	public void setLimitParameter(LimitServerResource limits) {
		// TODO Auto-generated method stub
		
	}
}
