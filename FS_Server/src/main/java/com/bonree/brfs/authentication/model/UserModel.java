package com.bonree.brfs.authentication.model;

import java.util.HashSet;
import java.util.Set;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:25:48
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 简单用户信息model
 ******************************************************************************/
public class UserModel {

    private String userName;

    private String passwd;

    private byte acl;

    private String description;

    private Set<Integer> storageList;

    public UserModel() {
        storageList = new HashSet<Integer>();
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public byte getAcl() {
        return acl;
    }

    public void setAcl(byte acl) {
        this.acl = acl;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void addStorageName(int storageIndex) {
        storageList.add(storageIndex);
    }

    public void removeStorageName(int storageIndex) {
        storageList.remove(storageIndex);
    }
    public Set<Integer> getStorageList() {
        return storageList;
    }

    public void setStorageList(Set<Integer> storageList) {
        this.storageList = storageList;
    }

    @Override
    public String toString() {
        return "UserModel [userName=" + userName + ", passwd=" + passwd + ", acl=" + acl + ", description=" + description + ", storageList=" + storageList + "]";
    }

}
