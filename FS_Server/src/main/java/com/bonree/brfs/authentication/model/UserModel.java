package com.bonree.brfs.authentication.model;

import java.util.HashSet;
import java.util.Set;

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
