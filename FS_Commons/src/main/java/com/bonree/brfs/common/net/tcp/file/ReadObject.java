package com.bonree.brfs.common.net.tcp.file;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ReadObject {
    @JsonIgnore
    public static final int RAW_PATH = 1;
    @JsonIgnore
    public static final int RAW_OFFSET = 2;
    @JsonIgnore
    public static final int RAW_LENGTH = 4;

    @JsonProperty("token")
    private int token;
    @JsonProperty("path")
    private String filePath;
    @JsonProperty("offset")
    private long offset;
    @JsonProperty("length")
    private int length;
    @JsonProperty("raw")
    private int raw;

    private int index;
    private String sn;
    private long time;
    private long duration;
    private String fileName;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getSn() {
        return sn;
    }

    public void setSn(String sn) {
        this.sn = sn;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getToken() {
        return token;
    }

    public void setToken(int token) {
        this.token = token;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getRaw() {
        return raw;
    }

    public void setRaw(int raw) {
        this.raw = raw;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{filepath=").append(filePath).append(",")
            .append("offset=").append(offset).append(",")
            .append("length=").append(length).append("}");

        return builder.toString();
    }
}
