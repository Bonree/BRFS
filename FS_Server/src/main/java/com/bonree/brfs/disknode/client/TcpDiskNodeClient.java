package com.bonree.brfs.disknode.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.core.util.CloseUtil;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.client.ResponseHandler;
import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.net.tcp.file.client.FileContentPart;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.disknode.boot.DataNodeBootStrap;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.tcp.handler.data.DeleteFileMessage;
import com.bonree.brfs.disknode.server.tcp.handler.data.FileRecoveryMessage;
import com.bonree.brfs.disknode.server.tcp.handler.data.ListFileMessage;
import com.bonree.brfs.disknode.server.tcp.handler.data.OpenFileMessage;
import com.bonree.brfs.disknode.server.tcp.handler.data.WriteFileData;
import com.bonree.brfs.disknode.server.tcp.handler.data.WriteFileMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.primitives.Longs;

public class TcpDiskNodeClient implements DiskNodeClient {
	private static final Logger LOG = LoggerFactory.getLogger(TcpDiskNodeClient.class);
	
	private TcpClient<BaseMessage, BaseResponse> client;
	private TcpClient<ReadObject, FileContentPart> readClient;
	
	public TcpDiskNodeClient(TcpClient<BaseMessage, BaseResponse> client) {
		this(client, null);
	}
	
	public TcpDiskNodeClient(TcpClient<BaseMessage, BaseResponse> client, TcpClient<ReadObject, FileContentPart> readClient) {
		this.client = client;
		this.readClient = readClient;
	}

	@Override
	public void close() throws IOException {
		CloseUtil.closeQuietly(client);
		CloseUtils.closeQuietly(readClient);
	}

	@Override
	public boolean ping() {
		try {
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_PING_PONG);
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			if(response != null && response.getCode() == ResponseCode.OK) {
				return true;
			}
		} catch (Exception e) {
			LOG.error("ping to server error", e);
		}
		
		return false;
	}

	@Override
	public long openFile(String path, long capacity) {
		try {
			OpenFileMessage openFileMessage = new OpenFileMessage();
			openFileMessage.setFilePath(path);
			openFileMessage.setCapacity(capacity);
			
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_OPEN_FILE);
			message.setBody(ProtoStuffUtils.serialize(openFileMessage));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			if(response != null && response.getCode() == ResponseCode.OK) {
				return Longs.fromByteArray(response.getBody());
			}
		} catch (Exception e) {
			LOG.error("open file error", e);
		}
		
		return -1;
	}

	@Override
	public long closeFile(String path) {
		try {
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_CLOSE_FILE);
			message.setBody(BrStringUtils.toUtf8Bytes(path));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			if(response != null && response.getCode() == ResponseCode.OK) {
				return Longs.fromByteArray(response.getBody());
			}
			
		} catch (Exception e) {
			LOG.error("close file error", e);
		}
		
		return -1;
	}

	@Override
	public WriteResult writeData(String path, byte[] bytes) throws IOException {
		try {
			WriteFileData data = new WriteFileData();
			data.setData(bytes);
			
			WriteFileMessage writeFileMessage = new WriteFileMessage();
			writeFileMessage.setFilePath(path);
			writeFileMessage.setDatas(new WriteFileData[]{data});
			
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_WRITE_FILE);
			message.setBody(ProtoStuffUtils.serialize(writeFileMessage));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			if(response != null && response.getCode() == ResponseCode.OK) {
				WriteResultList resultList = ProtoStuffUtils.deserialize(response.getBody(), WriteResultList.class);
				WriteResult[] results = resultList.getWriteResults();
				
				if(results.length == 1) {
					return results[0];
				}
			}
		} catch (Exception e) {
			LOG.error("write file error", e);
		}
		
		return null;
	}

	@Override
	public WriteResult writeData(String path, byte[] bytes, int offset, int size)
			throws IOException {
		int length = Math.min(size, bytes.length - offset);
		byte[] copy = new byte[length];
		
		System.arraycopy(bytes, offset, copy, 0, length);
		
		return writeData(path, copy);
	}

	@Override
	public WriteResult[] writeDatas(String path, List<byte[]> dataList) throws IOException {
		try {
			TimeWatcher tw = new TimeWatcher();
			WriteFileData[] datas = new WriteFileData[dataList.size()];
			for(int i = 0; i < datas.length; i++) {
				WriteFileData data = new WriteFileData();
				data.setData(dataList.get(i));
				
				datas[i] = data;
			}
			
			WriteFileMessage writeFileMessage = new WriteFileMessage();
			writeFileMessage.setFilePath(path);
			writeFileMessage.setDatas(datas);
			
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_WRITE_FILE);
			message.setBody(ProtoStuffUtils.serialize(writeFileMessage));
			
			LOG.info("TIME_TEST prepare data for file[{}] take {} ms", path, tw.getElapsedTimeAndRefresh());
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			LOG.debug("write [{}] datas to data node in file[{}]", dataList.size(), path);
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			
			LOG.info("TIME_TEST write datalist[{}] to file[{}] take {} ms", dataList.size(), path, tw.getElapsedTimeAndRefresh());
			
			if(response != null && response.getCode() == ResponseCode.OK) {
				WriteResultList resultList = ProtoStuffUtils.deserialize(response.getBody(), WriteResultList.class);
				
				LOG.info("TIME_TEST deserialize response for file[{}] take {} ms", path, tw.getElapsedTimeAndRefresh());
				
				return resultList.getWriteResults();
			}
		} catch (Exception e) {
			LOG.error("write file error", e);
		}
		
		return null;
	}

	@Override
	public boolean flush(String file) throws IOException {
		try {
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_FLUSH_FILE);
			message.setBody(BrStringUtils.toUtf8Bytes(file));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			
			if(response != null && response.getCode() == ResponseCode.OK) {
				return true;
			}
		} catch (Exception e) {
			LOG.error("flush file error", e);
		}
		
		return false;
	}

	@Override
	public void readData(String path, long offset, ByteConsumer consumer) throws IOException {
		readData(path, offset, Integer.MAX_VALUE, consumer);
	}

	@Override
	public void readData(String path, long offset, int size, ByteConsumer consumer) throws IOException {
		if(readClient == null) {
			throw new UnsupportedOperationException("no read client is set");
		}
		
		ReadObject object = new ReadObject();
		object.setFilePath(path);
		object.setOffset(offset);
		object.setLength(size);
		
		try {
			readClient.sendMessage(object, new ResponseHandler<FileContentPart>() {

				@Override
				public void handle(FileContentPart response) {
					consumer.consume(response.content(), response.endOfContent());
				}

				@Override
				public void error(Throwable t) {
					consumer.error(t);
				}
			});
		} catch (Exception e) {
			throw new IOException("can not send read message", e);
		}
	}
	
	@Override
	public void readFile(String path, ByteConsumer consumer) throws IOException {
		if(readClient == null) {
			throw new UnsupportedOperationException("no read client is set");
		}
		
		ReadObject object = new ReadObject();
		object.setFilePath(path);
		object.setOffset(0);
		object.setLength(Integer.MAX_VALUE);
		object.setRaw(ReadObject.RAW_OFFSET);
		
		try {
			readClient.sendMessage(object, new ResponseHandler<FileContentPart>() {

				@Override
				public void handle(FileContentPart response) {
					consumer.consume(response.content(), response.endOfContent());
				}

				@Override
				public void error(Throwable t) {
					consumer.error(t);
				}
			});
		} catch (Exception e) {
			throw new IOException("can not send read message", e);
		}
	}

	@Override
	public List<FileInfo> listFiles(String path, int level) {
		try {
			ListFileMessage listFileMessage = new ListFileMessage();
			listFileMessage.setPath(path);
			listFileMessage.setLevel(level);
			
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_LIST_FILE);
			message.setBody(ProtoStuffUtils.serialize(listFileMessage));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			if(response != null && response.getCode() == ResponseCode.OK) {
				return JsonUtils.toObject(response.getBody(), new TypeReference<List<FileInfo>>() {
				});
			}
		} catch (Exception e) {
			LOG.error("list files of dir[{}] with level[{}] error", path, level, e);
		}
		
		return null;
	}

	@Override
	public boolean deleteFile(String path, boolean force) {
		return deleteDir(path, force, false);
	}

	@Override
	public boolean deleteDir(String path, boolean force, boolean recursive) {
		try {
			DeleteFileMessage deleteFileMessage = new DeleteFileMessage();
			deleteFileMessage.setFilePath(path);
			deleteFileMessage.setForce(force);
			deleteFileMessage.setRecursive(recursive);
			
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_DELETE_FILE);
			message.setBody(ProtoStuffUtils.serialize(deleteFileMessage));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			if(response != null && response.getCode() == ResponseCode.OK) {
				return true;
			}
		} catch (Exception e) {
			LOG.error("delete file [{}] error", path, e);
		}
		
		return false;
	}

	@Override
	public long getFileLength(String path) {
		try {
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_METADATA);
			message.setBody(BrStringUtils.toUtf8Bytes(path));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			if(response != null && response.getCode() == ResponseCode.OK) {
				return Longs.fromByteArray(response.getBody());
			}
		} catch (Exception e) {
			LOG.error("get meta data from file [{}] error", path, e);
		}
		
		return -1;
	}

	@Override
	public boolean recover(String path, long fileLength, List<String> fullstates) {
		try {
			FileRecoveryMessage recoveryMessage = new FileRecoveryMessage();
			recoveryMessage.setFilePath(path);
			recoveryMessage.setOffset(fileLength);
			
			String[] states = new String[fullstates.size()];
			recoveryMessage.setSources(fullstates.toArray(states));
			
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_RECOVER_FILE);
			message.setBody(ProtoStuffUtils.serialize(recoveryMessage));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error(Throwable e) {
					future.completeExceptionally(e);
				}
			});
			
			BaseResponse response = future.get();
			if(response != null && response.getCode() == ResponseCode.OK) {
				return true;
			}
		} catch (Exception e) {
			LOG.error("get meta data from file [{}] error", path, e);
		}
		
		return false;
	}
}
