package com.bonree.brfs.disknode.server.handler;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import com.bonree.brfs.common.utils.BufferUtils;
import com.bonree.brfs.common.utils.ByteUtils;
import com.bonree.brfs.common.write.data.FileEncoder;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

public class CloseMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(CloseMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	private FileFormater fileFormater;

	public CloseMessageHandler(DiskContext context, FileWriterManager nodeManager, FileFormater fileFormater) {
		this.diskContext = context;
		this.writerManager = nodeManager;
		this.fileFormater = fileFormater;
	}

	@Override
	public void handle(HttpMessage msg, HandleResultCallback callback) {
		HandleResult result = new HandleResult();
		String filePath = null;
		try {
			filePath = diskContext.getConcreteFilePath(msg.getPath());
			LOG.info("CLOSE file[{}]", filePath);
			
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(filePath, false);
			if(binding == null) {
				LOG.info("no writer is found for file[{}], treat it as OK!", filePath);
				File dataFile = new File(filePath);
				if(!dataFile.exists()) {
					result.setData(Longs.toByteArray(0));
					result.setSuccess(true);
					return;
				}
				
				MappedByteBuffer buffer = Files.map(dataFile);
				try {
					buffer.position(fileFormater.fileHeader().length());
					buffer.limit(buffer.capacity() - fileFormater.fileTailer().length());
					result.setData(Longs.toByteArray(ByteUtils.crc(buffer)));
					result.setSuccess(true);
					return;
				} finally {
					BufferUtils.release(buffer);
				}
			}
			
			LOG.info("start writing file tailer for {}", filePath);
			binding.first().flush();
			byte[] fileBytes = DataFileReader.readFile(filePath, 2);
			long crcCode = ByteUtils.crc(fileBytes);
			LOG.info("final crc code[{}] by bytes[{}] of file[{}]", crcCode, fileBytes.length, filePath);
			
			byte[] tailer = Bytes.concat(FileEncoder.validate(crcCode), FileEncoder.tail());
			
			binding.first().write(tailer);
			binding.first().flush();
			
			LOG.info("close over for file[{}]", filePath);
			writerManager.close(filePath);
			
			result.setData(Longs.toByteArray(crcCode));
			result.setSuccess(true);
		} catch (IOException e) {
			result.setSuccess(false);
			LOG.error("close file[{}] error!", filePath, e);
		} finally {
			callback.completed(result);
		}
	}

	@Override
	public boolean isValidRequest(HttpMessage message) {
		return !message.getPath().isEmpty();
	}

}
