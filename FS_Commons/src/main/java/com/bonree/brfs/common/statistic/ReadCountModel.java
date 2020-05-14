package com.bonree.brfs.common.statistic;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.atomic.AtomicLong;

public class ReadCountModel {
    private AtomicLong readCount = new AtomicLong(0);
    private String srName;

    @JsonCreator
    public ReadCountModel(@JsonProperty("count") long count, @JsonProperty("srName") String srName) {
        this.readCount.set(count);
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

    public void addReadCount(long count) {
        if (count == 0) {
            return;
        }
        this.readCount.addAndGet(count);
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
