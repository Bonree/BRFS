package com.bonree.brfs.common.statistic;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.atomic.AtomicLong;

public class WriteCountModel {
    private AtomicLong writeCount = new AtomicLong(0);

    @JsonCreator
    public WriteCountModel(@JsonProperty("count") long count) {
        this.writeCount.set(count);
    }

    @JsonProperty("count")
    public long getWriteCount() {
        return writeCount.get();
    }

    public void addWriteCount() {
        this.writeCount.incrementAndGet();
    }

    public void addWriteCount(long count) {
        if (count == 0) {
            return;
        }
        this.writeCount.addAndGet(count);
    }

    public void print() {
        System.out.println(writeCount);
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("count", writeCount.get())
            .toString();
    }
}
