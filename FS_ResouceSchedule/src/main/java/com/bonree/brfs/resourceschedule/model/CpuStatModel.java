package com.bonree.brfs.resourceschedule.model;

import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.commons.ModelCalcInterface;
import com.bonree.brfs.resourceschedule.model.enums.CpuEnum;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class CpuStatModel extends AbstractResourceModel implements ModelCalcInterface<CpuStatModel>{
    /**
     * cpu使用率
     */
    private double cpuRate;
    /**
     * cpu未使用率
     */
    private double cpuRemainRate;
    /**
     * 合并次数
     */
    private int count = 1;

    public CpuStatModel(double cpuRate, double cpuRemainRate) {
        this.cpuRate = cpuRate;
        this.cpuRemainRate = cpuRemainRate;
    }

    public CpuStatModel() {
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(CpuEnum.CPU_RATE.name(), this.cpuRate);
    	obj.put(CpuEnum.CPU_REMAIN_RATE.name(), this.cpuRemainRate);
    	return obj;
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

	@Override
	public CpuStatModel calc(CpuStatModel t1) {
		return sum(t1);
	}

	@Override
	public CpuStatModel sum(CpuStatModel t1) {
		CpuStatModel obj = new CpuStatModel();
		if(t1 == null){
			obj.setCpuRate(this.cpuRate);
			obj.setCpuRemainRate(this.cpuRemainRate);
			obj.setCount(this.count);
		}else{
			obj.setCpuRate(obj.getCpuRate() + t1.getCpuRate());
			obj.setCpuRemainRate(obj.getCpuRemainRate() + t1.getCpuRemainRate());
			obj.setCount(this.count + 1);
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
