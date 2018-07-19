package com.bonree.brfs.duplication.storageregion;

import com.bonree.brfs.common.utils.Attributes;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.StorageConfigs;

public class StorageRegionConfig {
	public static final String CONFIG_ENABLE = "enable";
    public static final String CONFIG_REPLICATE_NUM = "replicate_num";
    public static final String CONFIG_DATA_TTL = "data_ttl";
    public static final String CONFIG_FILE_CAPACITY = "file_capacity";
    public static final String CONFIG_FILE_PARTITION_DURATION = "file_patition_duration";
    
    private static final boolean DEFAULT_ENABLE = true;
    private static final int DEFAULT_REPLICATE_NUM = 
    		Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_STORAGE_REGION_REPLICATE_COUNT);
    private static final String DEFAULT_DATA_TTL =
    		Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_STORAGE_REGION_DATA_TTL);
    private static final long DEFAULT_FILE_CAPACITY =
    		Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_STORAGE_REGION_FILE_CAPACITY);
    private static final String DEFAULT_FILE_PARTITION_DURATION =
    		Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_FILE_PATITION_DURATION);
    
    private Attributes attributes = new Attributes();
    
    public StorageRegionConfig() {
    	attributes.putBoolean(CONFIG_ENABLE, DEFAULT_ENABLE);
    	attributes.putInt(CONFIG_REPLICATE_NUM, DEFAULT_REPLICATE_NUM);
    	attributes.putString(CONFIG_DATA_TTL, DEFAULT_DATA_TTL);
    	attributes.putLong(CONFIG_FILE_CAPACITY, DEFAULT_FILE_CAPACITY);
    	attributes.putString(CONFIG_FILE_PARTITION_DURATION, DEFAULT_FILE_PARTITION_DURATION);
    }
    
    public StorageRegionConfig(StorageRegion region) {
    	attributes.putBoolean(CONFIG_ENABLE, region.isEnable());
    	attributes.putInt(CONFIG_REPLICATE_NUM, region.getReplicateNum());
    	attributes.putString(CONFIG_DATA_TTL, region.getDataTtl());
    	attributes.putLong(CONFIG_FILE_CAPACITY, region.getFileCapacity());
    	attributes.putString(CONFIG_FILE_PARTITION_DURATION, region.getFilePartitionDuration());
    }
    
    public void update(Attributes params) {
    	attributes.putBoolean(CONFIG_ENABLE, params.getBoolean(CONFIG_ENABLE, attributes.getBoolean(CONFIG_ENABLE)));
    	attributes.putInt(CONFIG_REPLICATE_NUM, params.getInt(CONFIG_REPLICATE_NUM, attributes.getInt(CONFIG_REPLICATE_NUM)));
    	attributes.putString(CONFIG_DATA_TTL, params.getString(CONFIG_DATA_TTL, attributes.getString(CONFIG_DATA_TTL)));
    	attributes.putLong(CONFIG_FILE_CAPACITY, params.getLong(CONFIG_FILE_CAPACITY, attributes.getLong(CONFIG_FILE_CAPACITY)));
    	attributes.putString(CONFIG_FILE_PARTITION_DURATION, params.getString(CONFIG_FILE_PARTITION_DURATION, DEFAULT_FILE_PARTITION_DURATION));
    }
    
    public boolean isEnable() {
    	return attributes.getBoolean(CONFIG_ENABLE, DEFAULT_ENABLE);
    }
    
    public int getReplicateNum() {
    	return attributes.getInt(CONFIG_REPLICATE_NUM, DEFAULT_REPLICATE_NUM);
    }
    
    public String getDataTtl() {
    	return attributes.getString(CONFIG_DATA_TTL, DEFAULT_DATA_TTL);
    }
    
    public long getFileCapacity() {
    	return attributes.getLong(CONFIG_FILE_CAPACITY, DEFAULT_FILE_CAPACITY);
    }
    
    public String getFilePartitionDuration() {
    	return attributes.getString(CONFIG_FILE_PARTITION_DURATION, DEFAULT_FILE_PARTITION_DURATION);
    }
}
