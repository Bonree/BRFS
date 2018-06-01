package com.bonree.brfs.common;

import org.apache.commons.lang3.StringUtils;

import com.bonree.brfs.common.exception.BRFSException;

public enum ReturnCode {

    SUCCESS(2000), // 成功
    STORAGE_EXIST_ERROR(4001), // storage name已经存在
    STORAGE_NAME_ERROR(40010),// storage name错误
    STORAGE_REPLICATION_ERROR(40011), // storage name副本错误
    STORAGE_TTL_ERROR(40012), // storage nameTTL错误
    STORAGE_NONEXIST_ERROR(4002),   // storage name不存在
    STORAGE_UPDATE_ERROR(4003),     // 更新storage name发生错误
    STORAGE_REMOVE_ERROR(4004), // 移除storage name发生错误
    STORAGE_OPT_ERROR(4005), // 创建storage name发生错误
    USER_FORBID(3001), // 用户不行
    INVALID_CODE(3002),
    USER_DELETE_TIME_ERROR(3003);

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

    public static ReturnCode valueOf(int code) {
        ReturnCode[] returnCodes = ReturnCode.values();
        for(ReturnCode returnCode:returnCodes) {
            if(returnCode.getCode()==code) {
                return returnCode;
            }
        }
        return ReturnCode.INVALID_CODE;
    }

    public static ReturnCode checkCode(String storageName, String codeStr) {
        ReturnCode code = ReturnCode.INVALID_CODE;
        if (StringUtils.isNumeric(codeStr)) {
            code = valueOf(Integer.valueOf(codeStr));
        } else {
            code = valueOf(codeStr);
        }
        if (code.equals(ReturnCode.STORAGE_EXIST_ERROR)) {
            throw new BRFSException(storageName + " already exist!!!!");
        } else if (code.equals(ReturnCode.STORAGE_NONEXIST_ERROR)) {
            throw new BRFSException(storageName + " is not exist!!!!");
        } else if (code.equals(ReturnCode.STORAGE_REMOVE_ERROR)) {
            throw new BRFSException(storageName + " is not disable!!!!");
        } else if (code.equals(ReturnCode.STORAGE_UPDATE_ERROR)) {
            throw new BRFSException(storageName + " update error!!!!");
        } else if (code.equals(ReturnCode.STORAGE_OPT_ERROR)) {
            throw new BRFSException(storageName + " operate error!!!!");
        } else if (code.equals(ReturnCode.STORAGE_NAME_ERROR)) {
            throw new BRFSException(storageName + " name illegal!!!!");
        } else if (code.equals(ReturnCode.STORAGE_REPLICATION_ERROR)) {
            throw new BRFSException(storageName + " replication illegal!!!!");
        } else if (code.equals(ReturnCode.STORAGE_TTL_ERROR)) {
            throw new BRFSException(storageName + " ttl illegal!!!!");
        } else if (code.equals(ReturnCode.USER_FORBID)) {
            throw new BRFSException("User is forbidden!!!!");
        } else if (code.equals(ReturnCode.INVALID_CODE)) {
            throw new BRFSException("invalid code!!!");
        }else if(code.equals(ReturnCode.USER_DELETE_TIME_ERROR)) {
        	throw new BRFSException(storageName + " delete data time error");
        }
        return code;
    }
}
