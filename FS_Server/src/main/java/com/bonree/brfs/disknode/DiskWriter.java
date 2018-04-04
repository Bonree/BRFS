package com.bonree.brfs.disknode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.timer.WheelTimer;
import com.bonree.brfs.common.timer.WheelTimer.Timeout;
import com.bonree.brfs.disknode.buf.MappedWriteBuffer;
import com.bonree.brfs.disknode.buf.WriteBuffer;
import com.bonree.brfs.disknode.record.RecordElement;
import com.bonree.brfs.disknode.record.RecordWriter;

/**
 * 写数据到磁盘文件的具体实现类。
 * 
 * ---not thread safe---
 */
public class DiskWriter implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(DiskWriter.class);
	
	//默认的缓存大小（字节）
	private static final int DEFAULT_BUF_SIZE = 3 * 1024 * 1024;
	
	private int position;
	private int writeLength;
	
	private String filePath;
	private RandomAccessFile accessFile;
	private WriteBuffer buffer;
	private CRC32 crc = new CRC32();
	
	private WriteWorker attachedWorker;
	
	private RecordWriter recordWriter;
	
	private static final int DEFAULT_TIMEOUT_SECONDS = 5;
	private static WheelTimer<DiskWriter> timeoutWheel = new WheelTimer<DiskWriter>(DEFAULT_TIMEOUT_SECONDS);
	
	static {
		timeoutWheel.setTimeout(new Timeout<DiskWriter>() {

			@Override
			public void timeout(DiskWriter target) {
				LOG.info("timeout---" + target);
				try {
					target.flushBuffer();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		timeoutWheel.start();
	}
	
	public DiskWriter(String filePath, boolean override, WriteWorker worker) throws IOException {
		this(new File(filePath), override, worker);
	}
	
	public DiskWriter(File file, boolean override, WriteWorker worker) throws IOException {
		if(file.exists() && !override) {
			throw new IOException("file[" + file.getAbsolutePath() + "] is existed, but cannot override it!");
		}
		
		this.filePath = file.getAbsolutePath();
		this.accessFile = new RandomAccessFile(file, "rw");
		this.buffer = new MappedWriteBuffer(accessFile, DEFAULT_BUF_SIZE);
		this.attachedWorker = worker;
		this.recordWriter = RecordWriter.get(new File(file.getParent(), file.getName() + RecordWriter.RECORD_FILE_EXTEND));
	}
	
	public WriteWorker worker() {
		return attachedWorker;
	}
	
	public String getFilePath() {
		return filePath;
	}
	
	public void beginWriting() {
		writeLength = 0;
		crc.reset();
	}
	
	public void backWriting() {
		try {
			buffer.flush();
			buffer.setFilePosition(position);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public InputResult endWriting() {
		RecordElement element = new RecordElement();
		try {
			int startPosition = position;
			position += writeLength;
			
			element.setOffset(startPosition);
			element.setSize(writeLength);
			element.setCrc(crc.getValue());
			
			return new InputResult(startPosition, writeLength);
		} finally {
			try {
				recordWriter.record(element);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void flushBuffer() throws IOException {
		buffer.flush();
	}
	
	public void write(byte[] inBytes) throws IOException {
		write(inBytes, 0, inBytes.length);
	}
	
	public void write(byte[] inBytes, int offset, int size) throws IOException {
		if(size > (inBytes.length - offset)) {
			size = (inBytes.length - offset);
		}
		
		crc.update(inBytes, offset, size);
		int remaining = size;
		while(remaining > 0) {
			int writed = buffer.write(inBytes, offset, remaining);
			offset += writed;
			remaining -= writed;
			
			if(writed == 0) {
				//到这说明buffer满了，给它洗刷刷一下
				buffer.flush();
			}
		}
		
		writeLength += size;
		timeoutWheel.update(this);
	}
	
	@Override
	public void close() throws IOException {
		buffer.flush();
		accessFile.setLength(position);
		accessFile.close();
		timeoutWheel.remove(this);
	}
}
