package com.bonree.brfs.duplication.datastream.file;

public interface FileLimiterFactory {
	FileLimiter create(long time, int storageId);
}
