package com.bonree.brfs.server;

public class ServerInfo {

    private String hostName;
    private String ip;
    private String singleIdentification;
    private String multiIdentification;
    private String virtualIdentification;
    private int port;
    private boolean init;

    public ServerInfo() {

    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
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

    public boolean isInit() {
        return init;
    }

    public void setInit(boolean init) {
        this.init = init;
    }

    public String getSingleIdentification() {
        return singleIdentification;
    }

    public void setSingleIdentification(String singleIdentification) {
        this.singleIdentification = singleIdentification;
    }

    public String getMultiIdentification() {
        return multiIdentification;
    }

    public void setMultiIdentification(String multiIdentification) {
        this.multiIdentification = multiIdentification;
    }

    public String getVirtualIdentification() {
        return virtualIdentification;
    }

    public void setVirtualIdentification(String virtualIdentification) {
        this.virtualIdentification = virtualIdentification;
    }

}
