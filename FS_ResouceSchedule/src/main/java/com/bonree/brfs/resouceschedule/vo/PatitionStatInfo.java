package com.bonree.brfs.resouceschedule.vo;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.PATITION_ENUM;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class PatitionStatInfo {
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

    public PatitionStatInfo(String mountPoint, long usedSize, long unusedSize, long writeDataSize, long readDataSize) {
        this.mountPoint = mountPoint;
        this.usedSize = usedSize;
        this.remainSize = unusedSize;
        this.writeDataSize = writeDataSize;
        this.readDataSize = readDataSize;
    }

    public PatitionStatInfo(String mountPoint, long unusedSize, long writeDataSize, long readDataSize) {
        this.mountPoint = mountPoint;
        this.remainSize = unusedSize;
        this.writeDataSize = writeDataSize;
        this.readDataSize = readDataSize;
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(PATITION_ENUM.MOUNT_POINT.name(), this.mountPoint);
    	obj.put(PATITION_ENUM.USED_SIZE.name(), this.usedSize);
    	obj.put(PATITION_ENUM.REMAIN_SIZE.name(), this.remainSize);
    	obj.put(PATITION_ENUM.WIRTE_DATA_SIZE.name(), this.writeDataSize);
    	obj.put(PATITION_ENUM.READ_DATA_SIZE.name(), this.readDataSize);
    	return obj;
    }
    public String toString(){
    	return toJSONObject().toString();
    }
    public String toJSONString(){
    	return toJSONObject().toJSONString();
    }

    public PatitionStatInfo() {
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
    
}
