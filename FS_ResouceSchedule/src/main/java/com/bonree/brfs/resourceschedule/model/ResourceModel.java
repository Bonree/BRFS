package com.bonree.brfs.resourceschedule.model;

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
	private double clientWriteRequest;
	private double clientReadRequest;
	private double clientMetaRequest;
	
	@Override
	public JSONObject toJSONObject() {
		// TODO Auto-generated method stub
		JSONObject obj = new JSONObject();
		obj.put(ServerCommonEnum.SERVER_ID.name(), this.serverId);
		obj.put(SceneEnum.CLIENT_WRITE_REQUEST.name(), this.clientWriteRequest);
		obj.put(SceneEnum.CLIENT_READ_REQUEST.name(), this.clientReadRequest);
		obj.put(SceneEnum.CLIENT_META_REQUEST.name(), this.clientMetaRequest);
		return obj;
	}
	public String toString(){
		return toJSONObject().toString();
	}
	public String toJSONString(){
		return toJSONObject().toJSONString();
	}

	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public double getClientWriteRequest() {
		return clientWriteRequest;
	}

	public void setClientWriteRequest(double clientWriteRequest) {
		this.clientWriteRequest = clientWriteRequest;
	}

	public double getClientReadRequest() {
		return clientReadRequest;
	}

	public void setClientReadRequest(double clientReadRequest) {
		this.clientReadRequest = clientReadRequest;
	}

	public double getClientMetaRequest() {
		return clientMetaRequest;
	}

	public void setClientMetaRequest(double clientMetaRequest) {
		this.clientMetaRequest = clientMetaRequest;
	}
	
}
