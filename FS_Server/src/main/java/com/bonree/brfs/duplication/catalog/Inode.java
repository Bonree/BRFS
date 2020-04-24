package com.bonree.brfs.duplication.catalog;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Inode {
    private String name;
    private String fid;
    private int nodeType;
    @JsonCreator
    public Inode(@JsonProperty("name")String name,
                 @JsonProperty("fid") String fid,
                 @JsonProperty("nodeType") int nodeType) {
        this.name = name;
        this.fid = fid;
        this.nodeType = nodeType;
    }
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("fid")
    public String getFid() {
        return fid;
    }

    public void setFid(String fid) {
        this.fid = fid;
    }

    @JsonProperty("nodeType")
    public int getNodeType() {
        return nodeType;
    }

    public void setNodeType(int nodeType) {
        this.nodeType = nodeType;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("name", name)
            .add("fid", fid)
            .add("nodeType", nodeType)
            .toString();
    }
}
