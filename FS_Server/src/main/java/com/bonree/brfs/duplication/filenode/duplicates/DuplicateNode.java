package com.bonree.brfs.duplication.filenode.duplicates;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

public class DuplicateNode implements Comparable<DuplicateNode> {
    private final String group;
    private final String id;
    /**
     * 二级serverId
     */
    private final String secondId;

    @JsonCreator
    public DuplicateNode(@JsonProperty("group") String group,
                         @JsonProperty("id") String id,
                         @JsonProperty("secondId") String secondId) {
        this.group = group;
        this.id = id;
        this.secondId = secondId;
    }

    @JsonProperty
    public String getGroup() {
        return group;
    }

    @JsonProperty
    public String getId() {
        return id;
    }

    @JsonProperty("secondId")
    public String getSecondId() {
        return secondId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((group == null) ? 0 : group.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((secondId == null) ? 0 : secondId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof DuplicateNode)) {
            return false;
        }

        DuplicateNode other = (DuplicateNode) obj;
        return Strings.nullToEmpty(group).equals(other.group)
            && Strings.nullToEmpty(id).equals(other.id) && Strings.nullToEmpty(secondId).equals(other.secondId);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{").append(group).append(", ").append(id).append(secondId).append("}");

        return builder.toString();
    }

    @Override
    public int compareTo(DuplicateNode value) {
        return this.getSecondId().compareTo(value.getSecondId());
    }
}
