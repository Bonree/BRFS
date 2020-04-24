package com.bonree.brfs.disknode.server.tcp.handler.data;

public class WriteFileMessage {
    private String filePath;
    private WriteFileData[] datas;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public WriteFileData[] getDatas() {
        return datas;
    }

    public void setDatas(WriteFileData[] datas) {
        this.datas = datas;
    }

}
