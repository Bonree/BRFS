package com.bonree.brfs.resourceschedule.model;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.commons.ModelCalcInterface;
import com.bonree.brfs.resourceschedule.model.enums.PatitionEnum;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class PatitionStatModel extends AbstractResourceModel implements ModelCalcInterface<PatitionStatModel>{
    /**
     * 文件系统挂载点
     */
    private String mountPoint;
    /**
     * 已使用空间大小，单位kb
     */
    private long usedSize;
    /**
     * 未使用空间大小，单位kb
     */
    private long remainSize;
    /**
     * 写入数据大小，单位byte
     */
    private long writeDataSize;
    /**
     * 读取数据大小，单位byte
     */
    private long readDataSize;
    /**
     * 读取速度
     */
    private long readSpeed;
    /**
     * 写入速度
     */
    private long writeSpeed;
    /**
     * 最大读取速度
     */
    private long readMaxSpeed;
    /**
     * 最大写入速度
     */
    private long writeMaxSpeed;
    /**
     * 合并次数
     */
    private int count = 1;

    public PatitionStatModel(String mountPoint, long usedSize, long unusedSize, long writeDataSize, long readDataSize) {
        this.mountPoint = mountPoint;
        this.usedSize = usedSize;
        this.remainSize = unusedSize;
        this.writeDataSize = writeDataSize;
        this.readDataSize = readDataSize;
    }

    public PatitionStatModel(String mountPoint, long unusedSize, long writeDataSize, long readDataSize) {
        this.mountPoint = mountPoint;
        this.remainSize = unusedSize;
        this.writeDataSize = writeDataSize;
        this.readDataSize = readDataSize;
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(PatitionEnum.MOUNT_POINT.name(), this.mountPoint);
    	obj.put(PatitionEnum.USED_SIZE.name(), this.usedSize);
    	obj.put(PatitionEnum.REMAIN_SIZE.name(), this.remainSize);
    	obj.put(PatitionEnum.WIRTE_DATA_SIZE.name(), this.writeDataSize);
    	obj.put(PatitionEnum.READ_DATA_SIZE.name(), this.readDataSize);
    	return obj;
    }

    public PatitionStatModel() {
    }

    public String getMountPoint() {
        return mountPoint;
    }

    public void setMountPoint(String mountPoint) {
        this.mountPoint = mountPoint;
    }

    public long getUsedSize() {
        return usedSize;
    }

    public void setUsedSize(long usedSize) {
        this.usedSize = usedSize;
    }

    public long getWriteDataSize() {
        return writeDataSize;
    }

    public void setWriteDataSize(long writeDataSize) {
        this.writeDataSize = writeDataSize;
    }

    public long getReadDataSize() {
        return readDataSize;
    }

    public void setReadDataSize(long readDataSize) {
        this.readDataSize = readDataSize;
    }

	public long getRemainSize() {
		return remainSize;
	}

	public void setRemainSize(long remainSize) {
		this.remainSize = remainSize;
	}

	@Override
	public PatitionStatModel calc(PatitionStatModel t1) {
		PatitionStatModel obj = new PatitionStatModel();
		obj.setMountPoint(this.mountPoint);
		obj.setRemainSize(this.remainSize);
		obj.setUsedSize(this.usedSize);
		obj.setReadDataSize(this.readDataSize);
		obj.setWriteDataSize(this.writeDataSize);
		if(t1 != null){
			obj.setReadSpeed(this.readDataSize - t1.getReadDataSize());
			obj.setWriteSpeed(this.writeDataSize - t1.getWriteDataSize());
		}else{
			obj.setReadSpeed(this.readDataSize);
			obj.setWriteSpeed(this.writeDataSize);
		}
		obj.setReadMaxSpeed(obj.getReadSpeed());
		obj.setWriteMaxSpeed(obj.getWriteSpeed());
		return obj;
	}

	@Override
	public PatitionStatModel sum(PatitionStatModel t1) {
		PatitionStatModel obj = new PatitionStatModel();
		obj.setMountPoint(this.mountPoint);
		obj.setRemainSize(this.remainSize);
		obj.setUsedSize(this.usedSize);
		obj.setReadDataSize(this.readDataSize);
		obj.setWriteDataSize(this.writeDataSize);
		count = this.count;
		if(t1 != null){
			obj.setCount(count + t1.getCount());
			obj.setReadSpeed(this.readSpeed + t1.getReadSpeed());
			obj.setWriteSpeed(this.writeSpeed + t1.getWriteSpeed());
			obj.setReadMaxSpeed(this.readMaxSpeed > t1.getReadMaxSpeed() ? this.readMaxSpeed : t1.getReadMaxSpeed());
			obj.setWriteMaxSpeed(this.writeMaxSpeed > t1.getWriteMaxSpeed() ? this.writeMaxSpeed : t1.getWriteMaxSpeed());
		}else{
			obj.setCount(count);
			obj.setReadSpeed(this.readSpeed);
			obj.setWriteSpeed(this.writeSpeed);
			obj.setReadMaxSpeed(this.readSpeed);
			obj.setWriteMaxSpeed(this.writeSpeed);
		}
		return obj;
	}

	public long getReadSpeed() {
		return readSpeed;
	}

	public void setReadSpeed(long readSpeed) {
		this.readSpeed = readSpeed;
	}

	public long getWriteSpeed() {
		return writeSpeed;
	}

	public void setWriteSpeed(long writeSpeed) {
		this.writeSpeed = writeSpeed;
	}

	public long getReadMaxSpeed() {
		return readMaxSpeed;
	}

	public void setReadMaxSpeed(long readMaxSpeed) {
		this.readMaxSpeed = readMaxSpeed;
	}

	public long getWriteMaxSpeed() {
		return writeMaxSpeed;
	}

	public void setWriteMaxSpeed(long writeMaxSpeed) {
		this.writeMaxSpeed = writeMaxSpeed;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
    
}
