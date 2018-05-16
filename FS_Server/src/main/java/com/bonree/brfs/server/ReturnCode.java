package com.bonree.brfs.server;

public enum ReturnCode {
    SUCCESS("SUCCESS", 2000), STORAGE_EXIST_ERROR("STORAGE_EXIST", 4001), STORAGE_NONEXIST_ERROR("STORAGE_NONEXIST", 4002), STORAGE_UPDATE_ERROR("STORAGE_UPDATE_ERROR", 4003), STORAGE_REMOVE_ERROR("STORAGE_REMOVE_ERROR", 4004);

    // 成员变量
    private String name;
    private int code;

    // 构造方法
    private ReturnCode(String name, int code) {
        this.name = name;
        this.code = code;
    }

    // 普通方法
    public static String getName(int code) {
        for (ReturnCode c : ReturnCode.values()) {
            if (c.getCode() == code) {
                return c.name;
            }
        }
        return null;
    }

    // get set 方法
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String codeDetail() {
        return this.code + ":" + this.name();
    }
}
