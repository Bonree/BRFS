package com.bonree.brfs.schedulers.utils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.ByteUtils;
import com.bonree.brfs.common.write.data.FSCode;
import com.bonree.brfs.disknode.client.DiskNodeClient.ByteConsumer;
import com.bonree.brfs.disknode.fileformat.impl.SimpleFileHeader;

public class LocalRandomFileConsumer implements ByteConsumer {
	private static final Logger LOG = LoggerFactory.getLogger("LocalConsumer");
	private CompletableFuture<Boolean> result = new CompletableFuture<>();
	private String localPath = null; 
	
	public LocalRandomFileConsumer(String localPath) {
		this.localPath = localPath;
		
	}
	@Override
	public void consume(byte[] bytes, boolean endOfConsume) {
		WriteLocalFileData1(bytes);
		if(endOfConsume) {
			result.complete(true);
		}

	}
	public void WriteLocalFileData1(byte[] data) {
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(this.localPath, "rw");
			if(file.length() >0) {
				file.seek(file.length()-1);
			}
			file.write(data);
		}
		catch ( IOException e) {
			e.printStackTrace();
		}finally {
			if(file != null) {
				try {
					file.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
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
