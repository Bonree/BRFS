package com.bonree.brfs.schedulers.utils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.ByteUtils;
import com.bonree.brfs.common.write.data.FSCode;
import com.bonree.brfs.disknode.client.DiskNodeClient.ByteConsumer;
import com.bonree.brfs.disknode.fileformat.impl.SimpleFileHeader;

public class LocalByteStreamConsumer implements ByteConsumer {
	private static final Logger LOG = LoggerFactory.getLogger("LocalConsumer");
	private CompletableFuture<Boolean> result = new CompletableFuture<>();
	private String localPath = null; 
	private int bufferSize = 5*1024*1024;
	private BufferedOutputStream output = null;
	private ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
	public LocalByteStreamConsumer(String localPath, int bufferSize) {
		this.localPath = localPath;
		this.bufferSize = bufferSize;
	}
	public LocalByteStreamConsumer(String localPath) {
		this.localPath = localPath;
	}
	@Override
	public void consume(byte[] bytes, boolean endOfConsume) {
		try {
			this.byteStream.write(bytes);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		if(endOfConsume) {
			wirteLocalFileData(this.byteStream.toByteArray());
			result.complete(true);
		}

	}
	public void wirteLocalFileData(byte[] data) {
		try {
			if(output == null) {
				output = new BufferedOutputStream(new FileOutputStream(localPath), bufferSize);
			}
			output.write(data);
			output.flush();
		}catch (FileNotFoundException e) {
			LOG.error("write byte error {}", e);
		} catch (IOException e) {
			LOG.error("write byte error {}", e);
		}finally {
			try {
				output.close();
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void error(Throwable e) {
		LOG.error("recovery file error!! clear data !!! {}", e);
		result.completeExceptionally(e);
	}
	public String getLocalPath() {
		return localPath;
	}
	public CompletableFuture<Boolean> getResult() {
		return result;
	}
	

}
