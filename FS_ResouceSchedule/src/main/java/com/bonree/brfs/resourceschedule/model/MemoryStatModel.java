package com.bonree.brfs.resourceschedule.model;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.model.enums.MemoryEnum;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 内存状态信息
 ******************************************************************************/
public class MemoryStatModel extends AbstractResourceModel{
    /**
     * 内存使用率
     */
    private double memoryRate;
    /**
     * 内存剩余使用率
     */
    private double memoryRemainRate;
	public MemoryStatModel(double memoryRate, double memoryRemainRate) {
		super();
		this.memoryRate = memoryRate;
		this.memoryRemainRate = memoryRemainRate;
	}
	public MemoryStatModel() {
	}
	public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(MemoryEnum.MEMORY_RATE.name(), this.memoryRate);
    	obj.put(MemoryEnum.MEMORY_RATE.name(), this.memoryRemainRate);
    	return obj;
    }

	public double getMemoryRate() {
		return memoryRate;
	}
	public void setMemoryRate(double memoryRate) {
		this.memoryRate = memoryRate;
	}
	public double getMemoryRemainRate() {
		return memoryRemainRate;
	}
	public void setMemoryRemainRate(double memoryRemainRate) {
		this.memoryRemainRate = memoryRemainRate;
	}
	
    
    
}
