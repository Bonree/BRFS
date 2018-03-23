package com.bonree.brfs.resourceschedule.model;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.commons.ModelCalcInterface;
import com.bonree.brfs.resourceschedule.model.enums.NetEnum;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class NetStatModel extends AbstractResourceModel implements ModelCalcInterface<NetStatModel>{
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
    /**
     * 接收速度
     */
    private long rSpeed;
    /**
     * 发送速度
     */
    private long tSpeed;
    /**
     * 最大接收速度
     */
    private long rMaxSpeed;
    /**
     * 最大发送速度
     */
    private long tMaxSpeed;
    /**
     * 累加次数
     */
    private int count = 1;

    public NetStatModel(String ipAddress, long rDataSize, long tDataSize) {
        this.ipAddress = ipAddress;
        this.rDataSize = rDataSize;
        this.tDataSize = tDataSize;
    }

    public NetStatModel() {
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(NetEnum.IP_ADDRESS.name(), this.ipAddress);
    	obj.put(NetEnum.R_DATA_SIZE.name(), this.rDataSize);
    	obj.put(NetEnum.T_DATA_SIZE.name(), this.tDataSize);
    	obj.put(NetEnum.MAX_R_SPEED.name(), this.rMaxSpeed);
    	obj.put(NetEnum.MAX_T_SPEED.name(), this.tMaxSpeed);
    	obj.put(NetEnum.T_SPEED.name(), this.tSpeed);
    	obj.put(NetEnum.R_SPEED.name(), this.rSpeed);
    	obj.put(NetEnum.SUM_COUNT.name(), this.count);
    	
    	return obj;
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
	
	@Override
	public NetStatModel calc(NetStatModel t1) {
		NetStatModel obj = new NetStatModel();
		obj.setIpAddress(this.ipAddress);
		obj.setrDataSize(this.rDataSize);
		obj.settDataSize(this.tDataSize);
		if(t1 !=null){
			obj.settSpeed(this.tDataSize - t1.gettDataSize());
			obj.setrSpeed(this.rDataSize - t1.getrDataSize());
		}else{
			obj.settSpeed(this.tDataSize);
			obj.setrSpeed(this.rDataSize);
		}
		obj.setrMaxSpeed(obj.getrSpeed());
		obj.settMaxSpeed(obj.gettSpeed());
		return obj;
	}

	@Override
	public NetStatModel sum(NetStatModel t1) {
		NetStatModel obj = new NetStatModel();
		obj.setIpAddress(this.ipAddress);
		obj.setrDataSize(this.rDataSize);
		obj.settDataSize(this.tDataSize);
		int count = this.count;
		if(t1 !=null){
			obj.settSpeed(this.tSpeed + t1.gettSpeed());
			obj.setrSpeed(this.rSpeed + t1.getrSpeed());
			obj.setrMaxSpeed(this.rMaxSpeed > t1.getrMaxSpeed() ? this.rMaxSpeed : t1.getrMaxSpeed());
			obj.settMaxSpeed(this.tMaxSpeed > t1.gettMaxSpeed()? this.tMaxSpeed : t1.gettMaxSpeed());
			count += t1.getCount();
		}else{
			obj.settSpeed(this.tDataSize);
			obj.setrSpeed(this.rDataSize);
			obj.setrMaxSpeed(this.rMaxSpeed);
			obj.settMaxSpeed(this.tMaxSpeed);
		}
		obj.setCount(count);
		return obj;
	}
	
	public long getNetAvgRSpeed(){
		return this.rSpeed/this.count;
	}
	public long getNetAvgTSpeed(){
		return this.tSpeed/this.count;
	}
	
	public long getrSpeed() {
		return rSpeed;
	}
	
	public void setrSpeed(long rSpeed) {
		this.rSpeed = rSpeed;
	}
	
	public long gettSpeed() {
		return tSpeed;
	}

	public void settSpeed(long tSpeed) {
		this.tSpeed = tSpeed;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public long getrMaxSpeed() {
		return rMaxSpeed;
	}

	public void setrMaxSpeed(long rMaxSpeed) {
		this.rMaxSpeed = rMaxSpeed;
	}

	public long gettMaxSpeed() {
		return tMaxSpeed;
	}

	public void settMaxSpeed(long tMaxSpeed) {
		this.tMaxSpeed = tMaxSpeed;
	}
	
}
