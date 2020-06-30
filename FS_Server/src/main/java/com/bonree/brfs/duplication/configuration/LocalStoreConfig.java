package com.bonree.brfs.duplication.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LocalStoreConfig {

    @JsonProperty("path")
    private String storePath = "data";

    public String getStorePath() {
        return storePath;
    }

    public void setStorePath(String storePath) {
        this.storePath = storePath;
    }
}
