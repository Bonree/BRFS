package com.bonree.brfs.resourceschedule.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.model.enums.SceneEnum;
import com.bonree.brfs.resourceschedule.model.enums.ServerCommonEnum;
/****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月16日 下午2:08:40
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 可用server模型
 *****************************************************************************
 */
public class ResourceModel extends AbstractResourceModel{
	private int serverId;
	/**
	 * 元数据操作指标
	 */
	private double clientMetaRequest;
	/**
	 * sn读取请求
	 */
	private Map<String, Double> clientSNReadMap = new ConcurrentHashMap<String, Double>();
	/**
	 * sn写入请求
	 */
	private Map<String, Double> clientSNWriteMap = new ConcurrentHashMap<String, Double>();
	/**
	 * sn 剩余空间率，针对单个机器
	 */
	private Map<String, Double> snRemainRate = new ConcurrentHashMap<String, Double>();
	
	@Override
	public JSONObject toJSONObject() {
		// TODO Auto-generated method stub
		JSONObject obj = new JSONObject();
		obj.put(ServerCommonEnum.SERVER_ID.name(), this.serverId);
		JSONObject readObj = new JSONObject();
		if(this.clientSNReadMap != null){
			for(Map.Entry<String, Double> entry : this.clientSNReadMap.entrySet()){
				readObj.put(entry.getKey(), entry.getValue());
			}
		}
		obj.put(SceneEnum.CLIENT_READ_REQUEST.name(), readObj);
		JSONObject writeObj = new JSONObject();
		if(this.clientSNWriteMap != null){
			for(Map.Entry<String, Double> entry : this.clientSNWriteMap.entrySet()){
				writeObj.put(entry.getKey(), entry.getValue());
			}
		}
		obj.put(SceneEnum.CLIENT_WRITE_REQUEST.name(), writeObj);
		obj.put(SceneEnum.CLIENT_META_REQUEST.name(), this.clientMetaRequest);
		return obj;
	}
	
	public void putClientSNRead(String sn, double value){
		this.clientSNReadMap.put(sn, value);
	}
	
	public void putClientSNWrite(String sn, double value){
		this.clientSNWriteMap.put(sn, value);
	}
	
	public void putSnRemainRate(String sn, double value){
		this.snRemainRate.put(sn, value);
	}
	
	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public double getClientMetaRequest() {
		return clientMetaRequest;
	}

	public void setClientMetaRequest(double clientMetaRequest) {
		this.clientMetaRequest = clientMetaRequest;
	}

	public Map<String, Double> getClientSNReadMap() {
		return clientSNReadMap;
	}

	public void setClientSNReadMap(Map<String, Double> clientSNReadMap) {
		this.clientSNReadMap = clientSNReadMap;
	}

	public Map<String, Double> getClientSNWriteMap() {
		return clientSNWriteMap;
	}

	public void setClientSNWriteMap(Map<String, Double> clientSNWriteMap) {
		this.clientSNWriteMap = clientSNWriteMap;
	}
	public Map<String, Double> getSnRemainRate() {
		return snRemainRate;
	}
	public void setSnRemainRate(Map<String, Double> snRemainRate) {
		this.snRemainRate = snRemainRate;
	}
	
}
