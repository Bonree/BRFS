package com.bonree.brfs.disknode.data.write.record;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bonree.brfs.disknode.data.write.BufferedFileWriter;
import com.bonree.brfs.disknode.data.write.DirectFileWriter;
import com.bonree.brfs.disknode.data.write.FileWriter;
import com.bonree.brfs.disknode.data.write.buf.ByteFileBuffer;

/**
 * 记录处理器管理类
 * 
 * @author yupeng
 *
 */
public class RecordCollectionManager {
	private Map<String, RecordCollection> collections = new HashMap<String, RecordCollection>();
	
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
		return collections.get(dataFilePath);
	}
	
	/**
	 * 获取与数据文件相对应的写入日志记录文件处理对象
	 * 
	 * @param dataFile 数据文件
	 * @return
	 */
	public RecordCollection getRecordCollection(File dataFile, int bufferSize) {
		return getRecordCollection(dataFile.getAbsolutePath(), bufferSize);
	}
	
	/**
	 * 获取与数据文件名相对应的写入日志记录文件处理对象
	 * 
	 * @param dataFilePath 数据文件路径
	 * @return
	 */
	public RecordCollection getRecordCollection(String dataFilePath, int bufferSize) {
		RecordCollection collection = collections.get(dataFilePath);
		
		if(collection == null) {
			synchronized (collections) {
				collection = collections.get(dataFilePath);
				if(collection == null) {
					File recordFile = RecordFileBuilder.buildFrom(dataFilePath);
					try {
						FileWriter writer = bufferSize > 0 ? new BufferedFileWriter(recordFile, new ByteFileBuffer(bufferSize))
						                                   : new DirectFileWriter(recordFile);
						
						collection = new RecordCollection(recordFile, writer, this);
						collections.put(dataFilePath, collection);
					} catch (IOException e) {
					}
				}
			}
		}
		
		return collection;
	}
	
	void releaseCollection(File dataFile) {
		synchronized (collections) {
			collections.remove(dataFile.getAbsolutePath());
		}
	}
}
