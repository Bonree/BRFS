package com.bonree.brfs.disknode.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.ResponseCode;
import com.bonree.brfs.common.net.tcp.client.ResponseHandler;
import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.disknode.boot.DataNodeBootStrap;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.tcp.handler.data.DeleteFileMessage;
import com.bonree.brfs.disknode.server.tcp.handler.data.FileRecoveryMessage;
import com.bonree.brfs.disknode.server.tcp.handler.data.ListFileMessage;
import com.bonree.brfs.disknode.server.tcp.handler.data.OpenFileMessage;
import com.bonree.brfs.disknode.server.tcp.handler.data.ReadFileMessage;
import com.bonree.brfs.disknode.server.tcp.handler.data.WriteFileData;
import com.bonree.brfs.disknode.server.tcp.handler.data.WriteFileMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.primitives.Longs;

public class TcpDiskNodeClient implements DiskNodeClient {
	private static final Logger LOG = LoggerFactory.getLogger(TcpDiskNodeClient.class);
	
	private TcpClient<BaseMessage, BaseResponse> client;
	
	public TcpDiskNodeClient(TcpClient<BaseMessage, BaseResponse> client) {
		this.client = client;
	}

	@Override
	public void close() throws IOException {
		client.close();
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
				public void error() {
					future.completeExceptionally(new Exception());
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
				public void error() {
					future.completeExceptionally(new Exception());
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
				public void error() {
					future.completeExceptionally(new Exception());
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
				public void error() {
					future.completeExceptionally(new Exception());
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
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			LOG.info("write [{}] datas to data node", dataList.size());
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error() {
					future.completeExceptionally(new Exception());
				}
			});
			
			BaseResponse response = future.get();
			
			if(response != null && response.getCode() == ResponseCode.OK) {
				WriteResultList resultList = ProtoStuffUtils.deserialize(response.getBody(), WriteResultList.class);
				
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
				public void error() {
					future.completeExceptionally(new Exception());
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
	public byte[] readData(String path, long offset) throws IOException {
		return readData(path, offset, Integer.MAX_VALUE);
	}

	@Override
	public byte[] readData(String path, long offset, int size)
			throws IOException {
		try {
			ReadFileMessage readFileMessage = new ReadFileMessage();
			readFileMessage.setFilePath(path);
			readFileMessage.setOffset(offset);
			readFileMessage.setLength(size);
			
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_READ_FILE);
			message.setBody(ProtoStuffUtils.serialize(readFileMessage));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error() {
					future.completeExceptionally(new Exception());
				}
			});
			
			BaseResponse response = future.get();
			
			if(response != null && response.getCode() == ResponseCode.OK) {
				return response.getBody();
			}
		} catch (Exception e) {
			LOG.error("read file error", e);
		}
		
		return null;
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
				public void error() {
					future.completeExceptionally(new Exception());
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
				public void error() {
					future.completeExceptionally(new Exception());
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
				public void error() {
					future.completeExceptionally(new Exception());
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
			
			BaseMessage message = new BaseMessage(DataNodeBootStrap.TYPE_DELETE_FILE);
			message.setBody(ProtoStuffUtils.serialize(recoveryMessage));
			
			CompletableFuture<BaseResponse> future = new CompletableFuture<BaseResponse>();
			client.sendMessage(message, new ResponseHandler<BaseResponse>() {
				
				@Override
				public void handle(BaseResponse response) {
					future.complete(response);
				}
				
				@Override
				public void error() {
					future.completeExceptionally(new Exception());
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

	@Override
	public void copyFrom(String host, int port, String remotePath, String localPath) throws Exception {
	}

	@Override
	public void copyTo(String host, int port, String localPath, String remotePath) throws Exception {
		
	}
}
