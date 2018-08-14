package com.bonree.brfs.disknode.server.tcp.handler;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.HandleCallback;
import com.bonree.brfs.common.net.tcp.MessageHandler;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.ByteUtils;
import com.bonree.brfs.common.write.data.FileEncoder;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.data.read.DataFileReader;
import com.bonree.brfs.disknode.data.write.FileWriterManager;
import com.bonree.brfs.disknode.data.write.RecordFileWriter;
import com.bonree.brfs.disknode.data.write.worker.WriteWorker;
import com.bonree.brfs.disknode.fileformat.FileFormater;
import com.bonree.brfs.disknode.utils.BufferUtils;
import com.bonree.brfs.disknode.utils.Pair;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

public class CloseFileMessageHandler implements MessageHandler {
	private static final Logger LOG = LoggerFactory.getLogger(CloseFileMessageHandler.class);
	
	private DiskContext diskContext;
	private FileWriterManager writerManager;
	private FileFormater fileFormater;

	public CloseFileMessageHandler(DiskContext context, FileWriterManager nodeManager, FileFormater fileFormater) {
		this.diskContext = context;
		this.writerManager = nodeManager;
		this.fileFormater = fileFormater;
	}

	@Override
	public void handleMessage(BaseMessage baseMessage, HandleCallback callback) {
		String path = BrStringUtils.fromUtf8Bytes(baseMessage.getBody());
		if(path == null) {
			callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR_PROTOCOL));
			return;
		}
		
		String filePath = null;
		try {
			filePath = diskContext.getConcreteFilePath(path);
			LOG.info("CLOSE file[{}]", filePath);
			
			Pair<RecordFileWriter, WriteWorker> binding = writerManager.getBinding(filePath, false);
			if(binding == null) {
				LOG.info("no writer is found for file[{}], treat it as OK!", filePath);
				
				File dataFile = new File(filePath);
				if(!dataFile.exists()) {
					callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR));
					return;
				}
				
				MappedByteBuffer buffer = Files.map(dataFile);
				buffer.position(fileFormater.fileHeader().length());
				buffer.limit(filePath.length() - fileFormater.fileTailer().length());
				BaseResponse response = new BaseResponse(baseMessage.getToken(), ResponseCode.OK);
				response.setBody(Longs.toByteArray(ByteUtils.cyc(buffer)));
				BufferUtils.release(buffer);
				callback.complete(response);
				return;
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
			
			BaseResponse response = new BaseResponse(baseMessage.getToken(), ResponseCode.OK);
			response.setBody(Longs.toByteArray(crcCode));
			callback.complete(response);
		} catch (IOException e) {
			LOG.error("close file[{}] error!", filePath);
			callback.complete(new BaseResponse(baseMessage.getToken(), ResponseCode.ERROR));
		}
	}

}
