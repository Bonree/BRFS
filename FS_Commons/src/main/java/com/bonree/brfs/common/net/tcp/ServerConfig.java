package com.bonree.brfs.common.net.tcp;

public class ServerConfig {
    private String host;
    private int port;
    private int backlog;
    private int bossThreadNums;
    private int workerThreadNums;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    public int getBossThreadNums() {
        return bossThreadNums;
    }

    public void setBossThreadNums(int bossThreadNums) {
        this.bossThreadNums = bossThreadNums;
    }

    public int getWorkerThreadNums() {
        return workerThreadNums;
    }

    public void setWorkerThreadNums(int workerThreadNums) {
        this.workerThreadNums = workerThreadNums;
    }
}
