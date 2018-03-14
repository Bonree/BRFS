package com.bonree.brfs.resouceschedule.vo;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.SERVER_COMMON_ENUM;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class ServerVo {
    /**
     * 服务基本信息
     */
    private BaseServerInfo baseServerInfo;
    /**
     * 服务状态信息
     */
    private Queue<ServerStatInfo> statInfoQueue = new ConcurrentLinkedQueue<ServerStatInfo>();

    public ServerVo(BaseServerInfo baseServerInfo) {
        this.baseServerInfo = baseServerInfo;
    }

    public ServerVo() {
    }

    public BaseServerInfo getBaseServerInfo() {
        return baseServerInfo;
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(SERVER_COMMON_ENUM.SERVER_BASE_INFO.name(), this.baseServerInfo.toJSONObject());
    	JSONArray statArray = new JSONArray();
    	ServerStatInfo stat = null;
    	int size = this.statInfoQueue.size();
    	for(int i = 0; i < size; i++ ){
    		stat = this.statInfoQueue.peek();
    		if(stat == null){
    			break;
    		}
    		statArray.add(stat.toJSONObject());
    	}
    	obj.put(SERVER_COMMON_ENUM.SERVER_STAT_INFO.name(), statArray);

    	return obj;
    }
    public String toString(){
    	return toJSONObject().toString();
    }
    public String toJSONString(){
    	return toJSONObject().toJSONString();
    }

    public void setBaseServerInfo(BaseServerInfo baseServerInfo) {
        this.baseServerInfo = baseServerInfo;
    }

    public Queue<ServerStatInfo> getStatInfoQueue() {
        return statInfoQueue;
    }

    public void setStatInfoQueue(Queue<ServerStatInfo> statInfoQueue) {
        this.statInfoQueue = statInfoQueue;
    }
    public void addServerStat(ServerStatInfo serverStatInfo){
        this.statInfoQueue.add(serverStatInfo);
    }
}
