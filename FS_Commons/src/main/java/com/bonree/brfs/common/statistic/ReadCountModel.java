package com.bonree.brfs.common.statistic;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.atomic.AtomicLong;

public class ReadCountModel {
    private AtomicLong readCount;
    private String srName;

    @JsonCreator
    public ReadCountModel(@JsonProperty("count") long count, @JsonProperty("srName") String srName) {
        this.readCount = new AtomicLong(count);
        this.srName = srName;
    }

    @JsonProperty("srName")
    public String getSrName() {
        return srName;
    }

    public void setSrName(String srName) {
        this.srName = srName;
    }

    @JsonProperty("count")
    public long getReadCount() {
        return readCount.get();
    }

    public void addReadCount() {
        this.readCount.incrementAndGet();
    }

    public void print() {
        System.out.println(readCount);
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("srName", srName)
            .add("count", readCount.get())
            .toString();
    }
}
