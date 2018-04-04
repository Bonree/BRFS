package com.bonree.brfs.resourceschedule.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetaResourceModel {
	private String serverId;
	private int cpuCoreCount;
	private long memoryTotalSize;
	private long memoryRemainSize;
	private double cpuRate;
	private double partitionTotalSize;
	private double partitionRemainSize;
	private Map<String,Long> partitionRemainMap = new ConcurrentHashMap<String,Long>();
	private Map<String,Long> partitionTotalMap = new ConcurrentHashMap<String,Long>();
	private Map<String,Long> partitionReadSpeedMap = new ConcurrentHashMap<String,Long>();
	private Map<String,Long> partitionWriteSpeedMap = new ConcurrentHashMap<String,Long>();
	private Map<String,Long> netRxSpeedMap = new ConcurrentHashMap<String,Long>();
	private Map<String,Long> netTxSpeedMap = new ConcurrentHashMap<String,Long>();
	
}
