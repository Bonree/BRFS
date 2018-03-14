package com.bonree.brfs.resourceschedule.model;

import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.model.ServerEnum.CPU_ENUM;
import com.bonree.brfs.resourceschedule.model.ServerEnum.MEMORY_ENUM;
import com.bonree.brfs.resourceschedule.model.ServerEnum.SERVER_COMMON_ENUM;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class CpuStatModel {
    /**
     * cpu使用率
     */
    private double cpuRate;
    /**
     * cpu未使用率
     */
    private double cpuRemainRate;

    public CpuStatModel(double cpuRate, double cpuRemainRate) {
        this.cpuRate = cpuRate;
        this.cpuRemainRate = cpuRemainRate;
    }

    public CpuStatModel() {
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(CPU_ENUM.CPU_RATE.name(), this.cpuRate);
    	obj.put(CPU_ENUM.CPU_REMAIN_RATE.name(), this.cpuRemainRate);
    	return obj;
    }
    public String toString(){
    	return toJSONObject().toString();
    }
    public String toJSONString(){
    	return toJSONObject().toJSONString();
    }

    public double getCpuRate() {
        return cpuRate;
    }

    public void setCpuRate(double cpuRate) {
        this.cpuRate = cpuRate;
    }

    public double getCpuRemainRate() {
        return cpuRemainRate;
    }

    public void setCpuRemainRate(double cpuRemainRate) {
        this.cpuRemainRate = cpuRemainRate;
    }
}
