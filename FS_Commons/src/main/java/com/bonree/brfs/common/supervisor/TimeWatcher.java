package com.bonree.brfs.common.supervisor;

public class TimeWatcher {
    private volatile long startTime;

    public TimeWatcher() {
        this.startTime = System.currentTimeMillis();
    }

    public int getElapsedTime() {
        return (int) (System.currentTimeMillis() - startTime);
    }

    public int getElapsedTimeAndRefresh() {
        try {
            return getElapsedTime();
        } finally {
            this.startTime = System.currentTimeMillis();
        }
    }
}
