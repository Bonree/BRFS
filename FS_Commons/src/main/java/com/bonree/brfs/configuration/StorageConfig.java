package com.bonree.brfs.configuration;

import com.bonree.brfs.common.utils.BrStringUtils;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月10日 下午3:47:09
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 针对SN的系统配置
 ******************************************************************************/
public class StorageConfig {

    private final int combinerFileSize;
    private final int dataTtl;
    private final int replication;
    private final boolean recover;
    private final boolean dataAchive;
    private final String achiverDFS;
    private final int achiverDelayTime;

    public StorageConfig(int combinerFileSize, int dataTtl, int replication, boolean recover, boolean dataAchive, String achiverDFS, int achiverDelayTime) {
        this.combinerFileSize = combinerFileSize;
        this.dataTtl = dataTtl;
        this.replication = replication;
        this.recover = recover;
        this.dataAchive = dataAchive;
        this.achiverDFS = achiverDFS;
        this.achiverDelayTime = achiverDelayTime;
    }

    public static StorageConfig parse(Configuration config) {
        String combinerFileSizeStr = config.getProperty(Configuration.STORAGE_COMBINER_FILE_MAX_SIZE, Configuration.STORAGE_COMBINER_FILE_MAX_SIZE_VALUE);
        String dataTtlStr = config.getProperty(Configuration.STORAGE_DATA_TTL, Configuration.STORAGE_DATA_TTL_VALUE);
        String replicationStr = config.getProperty(Configuration.STORAGE_REPLICATION_NUMBER, Configuration.STORAGE_REPLICATION_NUMBER_VALUE);
        String recoverStr = config.getProperty(Configuration.STORAGE_REPLICATION_RECOVER, Configuration.STORAGE_REPLICATION_RECOVER_VALUE);
        String dataAchiveStr = config.getProperty(Configuration.STORAGE_DATA_ACHIVE, Configuration.STORAGE_DATA_ACHIVE_VALUE);
        String achiverDFS = config.getProperty(Configuration.STORAGE_DATA_ACHIVE_DFS, Configuration.STORAGE_DATA_ACHIVE_DFS_VALUE);
        String achiverDelayTimeStr = config.getProperty(Configuration.STORAGE_DATA_ACHIVE_AFTER_TIME, Configuration.STORAGE_DATA_ACHIVE_AFTER_TIME_VALUE);
        int combinerFileSize = BrStringUtils.parseNumber(combinerFileSizeStr, Integer.class);
        int dataTtl = BrStringUtils.parseNumber(dataTtlStr, Integer.class);
        int replication = BrStringUtils.parseNumber(replicationStr, Integer.class);
        boolean recover = BrStringUtils.parseBoolean(recoverStr);
        boolean dataAchive = BrStringUtils.parseBoolean(dataAchiveStr);
        int achiverDelayTime = BrStringUtils.parseNumber(achiverDelayTimeStr, Integer.class);
        return new StorageConfig(combinerFileSize, dataTtl, replication, recover, dataAchive, achiverDFS, achiverDelayTime);
    }

    public int getCombinerFileSize() {
        return combinerFileSize;
    }

    public int getDataTtl() {
        return dataTtl;
    }

    public int getReplication() {
        return replication;
    }

    public boolean isRecover() {
        return recover;
    }

    public boolean isDataAchive() {
        return dataAchive;
    }

    public String getAchiverDFS() {
        return achiverDFS;
    }

    public int getAchiverDelayTime() {
        return achiverDelayTime;
    }

    @Override
    public String toString() {
        return "StorageConfig [combinerFileSize=" + combinerFileSize + ", dataTtl=" + dataTtl + ", replication=" + replication + ", recover=" + recover + ", dataAchive=" + dataAchive + ", achiverDFS=" + achiverDFS + ", achiverDelayTime=" + achiverDelayTime + "]";
    }

}
