package com.bonree.brfs.common;

import com.bonree.brfs.common.exception.BRFSException;

public enum ReturnCode {
    
    SUCCESS(2000), //成功
    STORAGE_EXIST_ERROR(4001), //storage name已经存在
    STORAGE_NAME_ERROR(40010),//storage name错误
    STORAGE_REPLICATION_ERROR(40011), //storage name副本错误
    STORAGE_TTL_ERROR(40012), //storage nameTTL错误
    STORAGE_NONEXIST_ERROR(4002),   //storage name不存在
    STORAGE_UPDATE_ERROR(4003),     //更新storage name发生错误
    STORAGE_REMOVE_ERROR(4004), //移除storage name发生错误
    STORAGE_OPT_ERROR(4005); //创建storage name发生错误

    // 成员变量
    private int code;

    // 构造方法
    private ReturnCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }
    
    public static ReturnCode checkCode(String storageName,ReturnCode code) {
        if(code.equals(ReturnCode.STORAGE_EXIST_ERROR)) {
            throw new BRFSException(storageName+" already exist!!!!");
        }else if(code.equals(ReturnCode.STORAGE_NONEXIST_ERROR)) {
            throw new BRFSException(storageName+" is not exist!!!!");
        }else if(code.equals(ReturnCode.STORAGE_REMOVE_ERROR)) {
            throw new BRFSException(storageName+" is not disable!!!!");
        }else if(code.equals(ReturnCode.STORAGE_UPDATE_ERROR)) {
            throw new BRFSException(storageName+" update error!!!!");
        }else if(code.equals(ReturnCode.STORAGE_OPT_ERROR)) {
            throw new BRFSException(storageName+" operate error!!!!");
        }else if(code.equals(ReturnCode.STORAGE_NAME_ERROR)) {
            throw new BRFSException(storageName+" name illegal!!!!");
        }else if(code.equals(ReturnCode.STORAGE_REPLICATION_ERROR)) {
            throw new BRFSException(storageName+" replication illegal!!!!");
        }else if(code.equals(ReturnCode.STORAGE_TTL_ERROR)) {
            throw new BRFSException(storageName+" ttl illegal!!!!");
        }
        return code;
    }
}
