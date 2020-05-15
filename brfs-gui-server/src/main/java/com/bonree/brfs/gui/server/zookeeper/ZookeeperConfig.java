package com.bonree.brfs.gui.server.zookeeper;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ZookeeperConfig {
    @JsonProperty("addresses")
    private String addresses = "localhost:2181";

    @JsonProperty("root")
    private String root = "/brfs";

    public String getAddresses() {
        return addresses;
    }

    public void setAddresses(String addresses) {
        this.addresses = addresses;
    }

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
    }
}
