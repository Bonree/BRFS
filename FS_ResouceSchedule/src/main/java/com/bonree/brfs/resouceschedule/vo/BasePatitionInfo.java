package com.bonree.brfs.resouceschedule.vo;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.NET_ENUM;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.PATITION_ENUM;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 文件系统基本信息
 ******************************************************************************/
public class BasePatitionInfo {
    /**
     * 文件系统挂载点
     */
    private String mountedPoint;
    /**
     * 文件系统格式
     */
    private String patitionFormateName;
    /**
     * 硬盘类型
     */
    private int diskType;
    /**
     * 文件系统大小 单位 kb
     */
    private long patitionSize;
    /**
     * 硬盘最大写入速度 单位byte
     */
    private long maxWriteSpeed;
    /**
     * 硬盘最大读取速度 单位byte
     */
    private long maxReadSpeed;

    public BasePatitionInfo(String mountedPoint, String patitionFormateName, int diskType, long patitionSize, long maxWriteSpeed, long maxReadSpeed) {
        this.mountedPoint = mountedPoint;
        this.patitionFormateName = patitionFormateName;
        this.diskType = diskType;
        this.patitionSize = patitionSize;
        this.maxWriteSpeed = maxWriteSpeed;
        this.maxReadSpeed = maxReadSpeed;
    }

    public BasePatitionInfo(String mountedPoint, String patitionFormateName, int diskType, long patitionSize) {
        this.mountedPoint = mountedPoint;
        this.patitionFormateName = patitionFormateName;
        this.diskType = diskType;
        this.patitionSize = patitionSize;
    }

    public BasePatitionInfo() {
    }

    public String getMountedPoint() {
        return mountedPoint;
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(PATITION_ENUM.MOUNT_POINT.name(), this.mountedPoint);
    	obj.put(PATITION_ENUM.PATITION_FORMAT.name(), this.patitionFormateName);
    	obj.put(PATITION_ENUM.DISK_TYPE.name(), this.diskType);
    	obj.put(PATITION_ENUM.PATITION_SIZE.name(), this.patitionSize);
    	obj.put(PATITION_ENUM.MAX_WRITE_SPEED.name(), this.maxWriteSpeed);
    	obj.put(PATITION_ENUM.MAX_READ_SPEED.name(), this.maxReadSpeed);
    	return obj;
    }
    public String toString(){
    	return toJSONObject().toString();
    }
    public String toJSONString(){
    	return toJSONObject().toJSONString();
    }

    public void setMountedPoint(String mountedPoint) {
        this.mountedPoint = mountedPoint;
    }

    public String getPatitionFormateName() {
        return patitionFormateName;
    }

    public void setPatitionFormateName(String patitionFormateName) {
        this.patitionFormateName = patitionFormateName;
    }

    public int getDiskType() {
        return diskType;
    }

    public void setDiskType(int diskType) {
        this.diskType = diskType;
    }

    public long getPatitionSize() {
        return patitionSize;
    }

    public void setPatitionSize(long patitionSize) {
        this.patitionSize = patitionSize;
    }

    public long getMaxWriteSpeed() {
        return maxWriteSpeed;
    }

    public void setMaxWriteSpeed(long maxWriteSpeed) {
        this.maxWriteSpeed = maxWriteSpeed;
    }

    public long getMaxReadSpeed() {
        return maxReadSpeed;
    }

    public void setMaxReadSpeed(long maxReadSpeed) {
        this.maxReadSpeed = maxReadSpeed;
    }
}
