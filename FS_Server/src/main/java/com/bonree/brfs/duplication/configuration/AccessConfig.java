package com.bonree.brfs.duplication.configuration;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Set;

public class AccessConfig {
    private final Set<String> allowed;
    private final Set<String> forbidden;

    @JsonCreator
    public AccessConfig(@JsonProperty("allowed") Set<String> allowed,
                        @JsonProperty("forbidden") Set<String> forbidden) {
        this.allowed = allowed;
        this.forbidden = forbidden;
    }

    @JsonProperty("allowed")
    public Set<String> getAllowed() {
        return allowed;
    }

    @JsonProperty("forbidden")
    public Set<String> getForbidden() {
        return forbidden;
    }

    @Override
    public String toString() {
        return toStringHelper(AccessConfig.class)
            .add("allowed", allowed)
            .add("forbidden", forbidden)
            .toString();
    }
}
