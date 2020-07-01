package com.bonree.brfs.disknode;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CompatibilityModelConfig {
    @JsonProperty("v1.model.switch")
    private boolean compatibilitySwitch = false;

    public boolean isCompatibilitySwitch() {
        return compatibilitySwitch;
    }

    public void setCompatibilitySwitch(boolean compatibilitySwitch) {
        this.compatibilitySwitch = compatibilitySwitch;
    }
}
