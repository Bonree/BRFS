package com.bonree.brfs.disknode.data.write.record;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.disknode.data.write.FileWriter;

/**
 * 数据写入记录处理器
 * 
 * @author yupeng
 *
 */
public class RecordCollection implements Closeable, Iterable<RecordElement> {
	private File recordFile;
	private FileWriter recordWriter;
	private RecordCollectionManager manager;
	
	/**
	 * 创建记录处理器
	 * 
	 * @param recordFilePath 记录文件路径
	 */
	RecordCollection(String recordFilePath, FileWriter fileWriter, RecordCollectionManager manager) {
		this(new File(recordFilePath), fileWriter, manager);
	}
	
	/**
	 * 创建记录处理器
	 * 
	 * @param recordFile 记录文件
	 */
	RecordCollection(File recordFile, FileWriter fileWriter, RecordCollectionManager manager) {
		this.recordFile = recordFile;
		this.recordWriter = fileWriter;
		this.manager = manager;
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
	
	@Override
	public Iterator<RecordElement> iterator() {
		InputStream input = null;
		try {
			input = new BufferedInputStream(new FileInputStream(recordFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return new RecordElementIterator(input);
	}

	@Override
	public void close() throws IOException {
		//写入记录关闭的时候说明记录已经不需要了，可以将
		//其关闭
		recordFile.delete();
		manager.releaseCollection(RecordFileBuilder.reverse(recordFile));
	}
	
	private class RecordElementIterator implements Iterator<RecordElement> {
		private InputStream recordInput;
		private RecordElement next;
		
		public RecordElementIterator(InputStream recordInput) {
			this.recordInput = recordInput;
			readNext();
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public RecordElement next() {
			RecordElement result = next;
			readNext();
			
			return result;
		}
		
		private void readNext() {
			next = ProtoStuffUtils.readFrom(recordInput, RecordElement.class);
		}
	}
}
