package com.bonree.brfs.configuration;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;

public class ConfigUnit<T> {
    private final String configName;
    private final T defaultValue;
    private Class<T> cls;

    private ConfigUnit(@Nonnull String name, T defaultValue, Class<T> cls) {
        Preconditions.checkNotNull(name, "the name of config unit can't be null!");
        this.configName = name;
        this.defaultValue = defaultValue;
        this.cls = cls;
    }

    public String name() {
        return this.configName;
    }

    public T defaultValue() {
        return this.defaultValue;
    }

    public Class<T> type() {
        return this.cls;
    }

    public static ConfigUnit<String> ofString(String name, String defaultValue) {
        return new ConfigUnit<String>(name, defaultValue, String.class);
    }

    public static ConfigUnit<Boolean> ofBoolean(String name, boolean defaultValue) {
        return new ConfigUnit<Boolean>(name, defaultValue, Boolean.class);
    }

    public static ConfigUnit<Integer> ofInt(String name, int defaultValue) {
        return new ConfigUnit<Integer>(name, defaultValue, Integer.class);
    }

    public static ConfigUnit<Long> ofLong(String name, long defaultValue) {
        return new ConfigUnit<Long>(name, defaultValue, Long.class);
    }

    public static ConfigUnit<Double> ofDouble(String name, double defaultValue) {
        return new ConfigUnit<Double>(name, defaultValue, Double.class);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{configName=").append(configName)
               .append(", defaultValue=").append(defaultValue)
               .append("}");

        return builder.toString();
    }
}
