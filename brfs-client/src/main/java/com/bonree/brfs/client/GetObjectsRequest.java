package com.bonree.brfs.client;

public class GetObjectsRequest {
    private final String srName;
    private final BRFSPath filePath;

    public GetObjectsRequest(String srName, BRFSPath filePath) {
        this.srName = srName;
        this.filePath = filePath;
    }

    public String getSrName() {
        return srName;
    }

    public BRFSPath getFilePath() {
        return filePath;
    }
}
