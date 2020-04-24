package com.bonree.brfs.configuration;

import org.apache.commons.configuration2.Configuration;

public class ConfigObj {
    private Configuration config;

    public ConfigObj(Configuration config) {
        this.config = config;
    }

    public <T> T GetConfig(ConfigUnit<T> unit) {
        T value = null;
        try {
            value = (T) config.get(unit.type(), unit.name(), unit.defaultValue());
        } catch (Exception e) {
            value = unit.defaultValue();
        }

        return value;
    }
}
