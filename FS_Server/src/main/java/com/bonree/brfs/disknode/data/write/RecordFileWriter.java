package com.bonree.brfs.disknode.data.write;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.supervisor.TimeWatcher;
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
	private static final Logger LOG = LoggerFactory.getLogger(RecordFileWriter.class);
	
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
		TimeWatcher tw = new TimeWatcher();
		RecordElement element = new RecordElement(delegate.position(), size, ByteUtils.crc(bytes, offset, size));
		recorder.put(element);
		
		LOG.info("TIME_TEST record for file[{}] take {} ms", delegate.getPath(), tw.getElapsedTimeAndRefresh());
		
		delegate.write(bytes, offset, size);
		
		LOG.info("TIME_TEST delegate for file[{}] take {} ms", delegate.getPath(), tw.getElapsedTimeAndRefresh());
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
