package com.bonree.brfs.disknode.data.write.record;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.disknode.data.write.BufferedFileWriter;
import com.bonree.brfs.disknode.data.write.DirectFileWriter;
import com.bonree.brfs.disknode.data.write.FileWriter;
import com.bonree.brfs.disknode.data.write.buf.ByteArrayFileBuffer;

/**
 * 记录处理器管理类
 * 
 * @author yupeng
 *
 */
public class RecordCollectionManager {
	private static final Logger LOG = LoggerFactory.getLogger(RecordCollectionManager.class);
	
	/**
	 * 以只读的方式打开日志记录，这种方式不会创建不存在
	 * 的{@link RecordCollection}, 如果不存在则返
	 * 回null
	 * 
	 * @param dataFile
	 * @return
	 */
	public RecordCollection getRecordCollectionReadOnly(File dataFile) {
		return getRecordCollectionReadOnly(dataFile.getAbsolutePath());
	}
	
	/**
	 * 以只读的方式打开日志记录，这种方式不会创建不存在
	 * 的{@link RecordCollection}, 如果不存在则返
	 * 回null
	 * 
	 * @param dataFilePath
	 * @return
	 */
	public RecordCollection getRecordCollectionReadOnly(String dataFilePath) {
		return new RecordCollection(RecordFileBuilder.buildFrom(dataFilePath), null, false);
	}
	
	/**
	 * 获取与数据文件相对应的写入日志记录文件处理对象
	 * 
	 * @param dataFile 数据文件
	 * @return
	 */
	public RecordCollection getRecordCollection(File dataFile, boolean append, int bufferSize, boolean deleteOnClose) {
		return getRecordCollection(dataFile.getAbsolutePath(), append, bufferSize, deleteOnClose);
	}
	
	/**
	 * 获取与数据文件名相对应的写入日志记录文件处理对象
	 * 
	 * @param dataFilePath 数据文件路径
	 * @return
	 */
	public RecordCollection getRecordCollection(String dataFilePath, boolean append, int bufferSize, boolean deleteOnClose) {
		File recordFile = RecordFileBuilder.buildFrom(dataFilePath);
		try {
			FileWriter writer = bufferSize > 0 ? new BufferedFileWriter(recordFile, append, new ByteArrayFileBuffer(bufferSize))
			                                   : new DirectFileWriter(recordFile, append);
			
			return new RecordCollection(recordFile, writer, deleteOnClose);
		} catch (IOException e) {
			LOG.error("getRecordCollection error", e);
		}
		
		return null;
	}
}
