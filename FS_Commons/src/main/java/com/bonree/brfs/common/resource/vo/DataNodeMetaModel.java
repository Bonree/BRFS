package com.bonree.brfs.common.resource.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.util.Map;

public class DataNodeMetaModel {
    @JsonProperty("serverId")
    private String serverID = null;
    @JsonProperty("mac")
    private String mac;
    @JsonProperty("ip")
    private String ip;
    @JsonProperty("port")
    private int port;
    @JsonProperty("partitionInfos")
    private Map<String, LocalPartitionInfo> partitionInfoMap;
    @JsonProperty("status")
    private NodeStatus status = NodeStatus.EMPTY;
    @JsonProperty("version")
    private NodeVersion version = NodeVersion.V2;

    public String getServerID() {
        return serverID;
    }

    public void setServerID(String serverID) {
        this.serverID = serverID;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Map<String, LocalPartitionInfo> getPartitionInfoMap() {
        return partitionInfoMap;
    }

    public void setPartitionInfoMap(Map<String, LocalPartitionInfo> partitionInfoMap) {
        this.partitionInfoMap = partitionInfoMap;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public void setStatus(NodeStatus status) {
        this.status = status;
    }

    public NodeVersion getVersion() {
        return version;
    }

    public void setVersion(NodeVersion version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("serverID", serverID)
                          .add("mac", mac)
                          .add("ip", ip)
                          .add("port", port)
                          .add("partitionInfoMap", partitionInfoMap)
                          .add("type", status)
                          .add("version", version)
                          .toString();
    }
}
