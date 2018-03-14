package com.bonree.brfs.resouceschedule.vo;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.NET_ENUM;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class NetStatInfo {
    /**
     * ip地址
     */
    private String ipAddress;
    /**
     * 网卡接收的数据大小，单位byte
     */
    private long rDataSize;
    /**
     * 网卡发送的数据大小，单位byte
     */
    private long tDataSize;

    public NetStatInfo(String ipAddress, long rDataSize, long tDataSize) {
        this.ipAddress = ipAddress;
        this.rDataSize = rDataSize;
        this.tDataSize = tDataSize;
    }

    public NetStatInfo() {
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(NET_ENUM.IP_ADDRESS.name(), this.ipAddress);
    	obj.put(NET_ENUM.R_DATA_SIZE.name(), this.rDataSize);
    	obj.put(NET_ENUM.T_DATA_SIZE.name(), this.tDataSize);
    	return obj;
    }
    public String toString(){
    	return toJSONObject().toString();
    }
    public String toJSONString(){
    	return toJSONObject().toJSONString();
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public long getrDataSize() {
        return rDataSize;
    }

    public void setrDataSize(long rDataSize) {
        this.rDataSize = rDataSize;
    }

    public long gettDataSize() {
        return tDataSize;
    }

    public void settDataSize(long tDataSize) {
        this.tDataSize = tDataSize;
    }
}
