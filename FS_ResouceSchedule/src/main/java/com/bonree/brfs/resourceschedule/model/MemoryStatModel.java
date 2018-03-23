package com.bonree.brfs.resourceschedule.model;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.commons.ModelCalcInterface;
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
public class MemoryStatModel extends AbstractResourceModel implements ModelCalcInterface<MemoryStatModel>{
    /**
     * 内存使用率
     */
    private double memoryRate;
    /**
     * 内存剩余使用率
     */
    private double memoryRemainRate;
    /**
     * 合并次数
     */
    private int count = 1;
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
	@Override
	public MemoryStatModel calc(MemoryStatModel t1) {
		return sum(t1);
	}
	@Override
	public MemoryStatModel sum(MemoryStatModel t1) {
		MemoryStatModel obj = new MemoryStatModel();
		if(t1 == null){
			obj.setMemoryRate(this.memoryRate);
			obj.setMemoryRemainRate(this.memoryRemainRate);
			obj.setCount(this.count);
		}else{
			obj.setMemoryRate(this.memoryRate + t1.getMemoryRate());
			obj.setMemoryRemainRate(this.memoryRemainRate + t1.getMemoryRemainRate());
			obj.setCount(this.count + t1.getCount());
		}
		return obj;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	    
}
