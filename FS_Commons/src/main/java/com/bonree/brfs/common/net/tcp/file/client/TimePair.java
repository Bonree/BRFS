package com.bonree.brfs.common.net.tcp.file.client;

public class TimePair {
    private final long time;
    private final long duration;

    public TimePair(long time, long duration){
        this.time = time;
        this.duration = duration;
    }

    public long getTime() {
        return time;
    }

    public long getDuration() {
        return duration;
    }

    public long time(){
        return this.time;
    }

    public long duration(){
        return this.duration;
    }

    @Override
    public int hashCode(){
        return (int) (this.time * 37 + this.duration);
    }

    @Override
    public boolean equals(Object obj){
        if(obj == null) {
            return false;
        }

        if(!(obj instanceof TimePair)) {
            return false;
        }

        TimePair oth = (TimePair) obj;

        return this.time == oth.time && this.duration == oth.duration;
    }
}
