package com.br.disknode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.br.disknode.buf.MappedWriteBuffer;
import com.br.disknode.buf.WriteBuffer;
import com.br.disknode.utils.TimeoutWheel;
import com.br.disknode.utils.TimeoutWheel.Timeout;

/**
 * 
 * 
 * not-threadsafe
 */
public class DiskWriter implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(DiskWriter.class);
	
	private static final int BUF_SIZE = 3 * 1024 * 1024;
	
	private int position;
	private int writeLength;
	
	private String filePath;
	private RandomAccessFile accessFile;
	private WriteBuffer buffer;
	
	private WriteWorker attachedWorker;
	
	private static final int TIMEOUT_SECONDS = 5;
	private static TimeoutWheel<DiskWriter> timeoutWheel = new TimeoutWheel<DiskWriter>(TIMEOUT_SECONDS);
	
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
		accessFile = new RandomAccessFile(file, "rw");
		buffer = new MappedWriteBuffer(accessFile, BUF_SIZE);
		attachedWorker = worker;
	}
	
	public WriteWorker worker() {
		return attachedWorker;
	}
	
	public String getFilePath() {
		return filePath;
	}
	
	public void beginWriting() {
		writeLength = 0;
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
		int startPosition = position;
		position += writeLength;
		return new InputResult(startPosition, writeLength);
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
