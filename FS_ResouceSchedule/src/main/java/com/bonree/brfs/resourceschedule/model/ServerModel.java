package com.bonree.brfs.resourceschedule.model;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.model.enums.ServerCommonEnum;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class ServerModel extends AbstractResourceModel{
    /**
     * 服务基本信息
     */
    private BaseServerModel baseServerInfo;
    /**
     * 服务状态信息
     */
    private Queue<ServerStatModel> statInfoQueue = new ConcurrentLinkedQueue<ServerStatModel>();

    public ServerModel(BaseServerModel baseServerInfo) {
        this.baseServerInfo = baseServerInfo;
    }

    public ServerModel() {
    }

    public BaseServerModel getBaseServerInfo() {
        return baseServerInfo;
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(ServerCommonEnum.SERVER_BASE_INFO.name(), this.baseServerInfo.toJSONObject());
    	JSONArray statArray = new JSONArray();
    	ServerStatModel stat = null;
    	int size = this.statInfoQueue.size();
    	for(int i = 0; i < size; i++ ){
    		stat = this.statInfoQueue.peek();
    		if(stat == null){
    			break;
    		}
    		statArray.add(stat.toJSONObject());
    	}
    	obj.put(ServerCommonEnum.SERVER_STAT_INFO.name(), statArray);

    	return obj;
    }

    public void setBaseServerInfo(BaseServerModel baseServerInfo) {
        this.baseServerInfo = baseServerInfo;
    }

    public Queue<ServerStatModel> getStatInfoQueue() {
        return statInfoQueue;
    }

    public void setStatInfoQueue(Queue<ServerStatModel> statInfoQueue) {
        this.statInfoQueue = statInfoQueue;
    }
    public void addServerStat(ServerStatModel serverStatInfo){
        this.statInfoQueue.add(serverStatInfo);
    }
}
