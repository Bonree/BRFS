package com.bonree.brfs.disknode.data.write.record;

import java.io.File;

/**
 * 数据写入日志文件名构建类
 * 
 * @author yupeng
 *
 */
public final class RecordFileBuilder {
	private static final String RECORD_FILE_SUFFIX = ".rd";
	
	/**
	 * 根据数据文件的文件名构建数据日志文件
	 * 
	 * @param dataFilePath
	 * @return 数据日志文件名
	 */
	public static File buildFrom(String dataFilePath) {
		return buildFrom(new File(dataFilePath));
	}
	
	/**
	 * 根据数据文件构建数据日志文件
	 * 
	 * @param dataFilePath
	 * @return 数据日志文件名
	 */
	public static File buildFrom(File dataFile) {
		StringBuilder recordFileNameBuilder = new StringBuilder();
		recordFileNameBuilder.append(dataFile.getName()).append(RECORD_FILE_SUFFIX);
		return new File(dataFile.getParent(), recordFileNameBuilder.toString());
	}
	
	/**
	 * 根据日志记录文件名获取数据文件
	 * 
	 * @param recordFilePaht
	 * @return
	 */
	public static File reverse(String recordFilePath) {
		return reverse(new File(recordFilePath));
	}
	
	/**
	 * 根据日志记录文件获取数据文件
	 * 
	 * @param recordFilePaht
	 * @return
	 */
	public static File reverse(File recordFile) {
		String recordFileName = recordFile.getName();
		if(!recordFileName.endsWith(RECORD_FILE_SUFFIX)) {
			throw new IllegalArgumentException(recordFile.getAbsolutePath());
		}
		
		return new File(recordFile.getParent(), recordFileName.substring(0, recordFileName.length() - RECORD_FILE_SUFFIX.length()));
	}
}
