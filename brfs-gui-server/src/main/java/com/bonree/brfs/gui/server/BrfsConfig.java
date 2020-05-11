package com.bonree.brfs.gui.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;

public class BrfsConfig {
    @JsonProperty("regionnode.address")
    private List<String> regionAddress = ImmutableList.of();
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;
    @JsonProperty("datanode.http.port")
    private int dataNodePort = 9999;

    public BrfsConfig() {
    }

    public List<String> getRegionAddress() {
        return regionAddress;
    }

    public void setRegionAddress(List<String> regionAddress) {
        this.regionAddress = regionAddress;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDataNodePort() {
        return dataNodePort;
    }

    public void setDataNodePort(int dataNodePort) {
        this.dataNodePort = dataNodePort;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BrfsConfig{");
        sb.append("regionAddress=").append(regionAddress);
        sb.append(", username='").append(username).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", dataNodePort=").append(dataNodePort);
        sb.append('}');
        return sb.toString();
    }
}
