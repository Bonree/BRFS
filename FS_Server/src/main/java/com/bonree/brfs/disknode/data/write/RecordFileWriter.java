package com.bonree.brfs.disknode.data.write;

import java.io.IOException;

import com.bonree.brfs.common.utils.ByteUtils;
import com.bonree.brfs.disknode.data.write.record.RecordCollection;
import com.bonree.brfs.disknode.data.write.record.RecordElement;

/**
 * 对数据写入进行日志记录的{@link FileWriter}包装类
 * 
 * @author yupeng
 *
 */
public class RecordFileWriter implements FileWriter {
	private FileWriter delegate;
	private RecordCollection recorder;
	
	public RecordFileWriter(RecordCollection recorder, FileWriter delegate) {
		this.recorder = recorder;
		this.delegate = delegate;
	}
	
	@Override
	public String getPath() {
		return delegate.getPath();
	}
	
	public RecordCollection getRecordCollection() {
		return recorder;
	}

	@Override
	public void write(byte[] bytes) throws IOException {
		write(bytes, 0, bytes.length);
	}

	@Override
	public void write(byte[] bytes, int offset, int size) throws IOException {
		RecordElement element = new RecordElement(delegate.position(), size, ByteUtils.crc(bytes, offset, size));
		recorder.put(element);
		
		delegate.write(bytes, offset, size);
	}

	@Override
	public void flush() throws IOException {
		recorder.sync();
		delegate.flush();
	}

	@Override
	public long position() {
		return delegate.position();
	}
	
	@Override
	public void close() throws IOException {
		delegate.close();
		recorder.close();
	}

	@Override
	public void position(long pos) throws IOException {
		delegate.position(pos);
	}

}
