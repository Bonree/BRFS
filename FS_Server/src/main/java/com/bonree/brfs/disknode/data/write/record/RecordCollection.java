package com.bonree.brfs.disknode.data.write.record;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.disknode.data.write.FileWriter;

/**
 * 数据写入记录处理器
 * 
 * @author yupeng
 *
 */
public class RecordCollection implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(RecordCollection.class);
	
	private File recordFile;
	private FileWriter recordWriter;
	private List<InputStream> openedStreams = new LinkedList<InputStream>();
	
	private boolean deleteOnClose;
	
	/**
	 * 创建记录处理器
	 * 
	 * @param recordFilePath 记录文件路径
	 */
	RecordCollection(String recordFilePath, FileWriter fileWriter, boolean deleteOnClose) {
		this(new File(recordFilePath), fileWriter, deleteOnClose);
	}
	
	/**
	 * 创建记录处理器
	 * 
	 * @param recordFile 记录文件
	 */
	RecordCollection(File recordFile, FileWriter fileWriter, boolean deleteOnClose) {
		this.recordFile = recordFile;
		this.recordWriter = fileWriter;
		this.deleteOnClose = deleteOnClose;
	}
	
	public File recordFile() {
		return recordFile;
	}
	
	/**
	 * 记录数据写入日志
	 * 
	 * @param element
	 * @throws IOException
	 */
	public void put(RecordElement element) throws IOException {
		recordWriter.write(ProtoStuffUtils.serialize(element));
	}
	
	/**
	 * 同步数据到文件
	 * 
	 * @throws IOException
	 */
	public void sync() throws IOException {
		recordWriter.flush();
	}
	
	public RecordElementReader getRecordElementReader() {
		return new RecordElementReader(recordFile);
	}

	@Override
	public void close() throws IOException {
		//写入记录关闭的时候说明记录已经不需要了，可以将
		//其关闭
		CloseUtils.closeQuietly(recordWriter);
		for(InputStream input : openedStreams) {
			CloseUtils.closeQuietly(input);
		}
		
		if(deleteOnClose) {
			LOG.info("It's time to delete record file[{}]", recordFile.getAbsolutePath());
			recordFile.delete();
		}
	}
}
