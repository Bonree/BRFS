package com.bonree.brfs.rocksdb;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 16:50
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: RocksDB配置信息
 ******************************************************************************/
public class RocksDBConfig {
    private int maxBackgroundFlush;

    private int maxBackgroundCompaction;

    private int maxOpenFiles;

    private int maxSubCompaction;

    private int blockCache;

    private int writeBufferSize;

    private int maxWriteBufferNumber;

    private int minWriteBufferNumToMerge;

    private int level0FileNumCompactionTrigger;

    private int targetFileSizeBase;

    private int maxBytesLevelBase;

    public int getMaxBackgroundFlush() {
        return maxBackgroundFlush;
    }

    public int getMaxBackgroundCompaction() {
        return maxBackgroundCompaction;
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }

    public int getMaxSubCompaction() {
        return maxSubCompaction;
    }

    public int getBlockCache() {
        return blockCache;
    }

    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    public int getMaxWriteBufferNumber() {
        return maxWriteBufferNumber;
    }

    public int getMinWriteBufferNumToMerge() {
        return minWriteBufferNumToMerge;
    }

    public int getLevel0FileNumCompactionTrigger() {
        return level0FileNumCompactionTrigger;
    }

    public int getTargetFileSizeBase() {
        return targetFileSizeBase;
    }

    public int getMaxBytesLevelBase() {
        return maxBytesLevelBase;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private RocksDBConfig rocksDBConfig;

        private Builder() {
            this.rocksDBConfig = new RocksDBConfig();
        }

        public Builder setMaxBackgroundFlush(int maxBackgroundFlush) {
            this.rocksDBConfig.maxBackgroundFlush = maxBackgroundFlush;
            return this;
        }

        public Builder setMaxBackgroundCompaction(int maxBackgroundCompaction) {
            this.rocksDBConfig.maxBackgroundCompaction = maxBackgroundCompaction;
            return this;
        }

        public Builder setMaxOpenFiles(int maxOpenFiles) {
            this.rocksDBConfig.maxOpenFiles = maxOpenFiles;
            return this;
        }

        public Builder setMaxSubCompaction(int maxSubCompaction) {
            this.rocksDBConfig.maxSubCompaction = maxSubCompaction;
            return this;
        }

        public Builder setBlockCache(int blockCache) {
            this.rocksDBConfig.blockCache = blockCache;
            return this;
        }

        public Builder setWriteBufferSize(int writeBufferSize) {
            this.rocksDBConfig.writeBufferSize = writeBufferSize;
            return this;
        }

        public Builder setMaxWriteBufferNumber(int maxWriteBufferNumber) {
            this.rocksDBConfig.maxWriteBufferNumber = maxWriteBufferNumber;
            return this;
        }

        public Builder setMinWriteBufferNumToMerge(int minWriteBufferNumToMerge) {
            this.rocksDBConfig.minWriteBufferNumToMerge = minWriteBufferNumToMerge;
            return this;
        }

        public Builder setLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger) {
            this.rocksDBConfig.level0FileNumCompactionTrigger = level0FileNumCompactionTrigger;
            return this;
        }

        public Builder setTargetFileSizeBase(int targetFileSizeBase) {
            this.rocksDBConfig.targetFileSizeBase = targetFileSizeBase;
            return this;
        }

        public Builder setMaxBytesLevelBase(int maxBytesLevelBase) {
            this.rocksDBConfig.maxBytesLevelBase = maxBytesLevelBase;
            return this;
        }

        public RocksDBConfig build() {
            return this.rocksDBConfig;
        }
    }

    @Override
    public String toString() {
        return "RocksDBConfig{" +
                "maxBackgroundFlush=" + maxBackgroundFlush +
                ", maxBackgroundCompaction=" + maxBackgroundCompaction +
                ", maxOpenFiles=" + maxOpenFiles +
                ", maxSubCompaction=" + maxSubCompaction +
                ", blockCache=" + blockCache +
                ", writeBufferSize=" + writeBufferSize +
                ", maxWriteBufferNumber=" + maxWriteBufferNumber +
                ", minWriteBufferNumToMerge=" + minWriteBufferNumToMerge +
                ", level0FileNumCompactionTrigger=" + level0FileNumCompactionTrigger +
                ", targetFileSizeBase=" + targetFileSizeBase +
                ", maxBytesLevelBase=" + maxBytesLevelBase +
                '}';
    }
}
