package com.bonree.brfs.disknode.client;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.net.http.client.ClientConfig;
import com.bonree.brfs.common.net.http.client.HttpClient;
import com.bonree.brfs.common.net.http.client.HttpResponse;
import com.bonree.brfs.common.net.http.client.URIBuilder;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.server.handler.data.FileCopyMessage;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteDataList;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.primitives.Longs;

public class HttpDiskNodeClient implements DiskNodeClient {
	private static final Logger LOG = LoggerFactory.getLogger(HttpDiskNodeClient.class);
	
	private static final String DEFAULT_SCHEME = "http";
	
	private HttpClient client;

	private String host;
	private int port;
	
	public HttpDiskNodeClient(String host, int port) {
		this(host, port, ClientConfig.DEFAULT);
	}
	
	public HttpDiskNodeClient(String host, int port, ClientConfig clientConfig) {
		this.host = host;
		this.port = port;
		this.client = new HttpClient(clientConfig);
	}
	
	@Override
	public boolean ping() {
		URI uri = new URIBuilder()
		.setScheme(DEFAULT_SCHEME)
		.setHost(host)
		.setPort(port)
		.setPath(DiskContext.URI_PING_PONG_ROOT + "/")
		.build();
		
		try {
			HttpResponse response = client.executeGet(uri);
			return response.isReponseOK();
		} catch (Exception e) {
			LOG.error("ping to {}:{} error", host, port, e);
		}
		
		return false;
	}
	
	@Override
	public long openFile(String path, long capacity) {
		URI uri = new URIBuilder()
		.setScheme(DEFAULT_SCHEME)
		.setHost(host)
		.setPort(port)
		.setPath(DiskContext.URI_DISK_NODE_ROOT + path)
		.addParameter("capacity", String.valueOf(capacity))
		.build();
		
		try {
			LOG.info("open file[{}] @ {}:{}", path, host, port);
			HttpResponse response = client.executePut(uri);
			LOG.debug("open file[{}] response[{}]", path, response.getStatusCode());
			if(response != null && response.isReponseOK()) {
				return Longs.fromByteArray(response.getResponseBody());
			}
		} catch (Exception e) {
			LOG.error("open file[{}] at {}:{} error", path, host, port, e);
		}
		
		return -1l;
	}

	@Override
	public WriteResult writeData(String path, byte[] bytes) throws IOException {
		List<byte[]> datas = new ArrayList<byte[]>();
		datas.add(bytes);
		
		WriteResult[] results = writeDatas(path, datas);
		
		return results != null ? results[0] : null;
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
		WriteDataList datas = new WriteDataList();
		WriteData[] dataArray = new WriteData[dataList.size()];
		for(int i = 0; i < dataArray.length; i++) {
			WriteData data = new WriteData();
			data.setBytes(dataList.get(i));
			dataArray[i] = data;
		}
		
		datas.setDatas(dataArray);
		
		URI uri = new URIBuilder()
		.setScheme(DEFAULT_SCHEME)
		.setHost(host)
		.setPort(port)
		.setPath(DiskContext.URI_DISK_NODE_ROOT + path)
		.build();
		
		try {
			LOG.info("write file[{}] with {} datas to {}:{}", path, dataList.size(), host, port);
			HttpResponse response = client.executePost(uri, ProtoStuffUtils.serialize(datas));
			LOG.debug("write file[{}] response[{}]", path, response.getStatusCode());
			if(response.isReponseOK()) {
				WriteResultList resultList = ProtoStuffUtils.deserialize(response.getResponseBody(), WriteResultList.class);
				return resultList.getWriteResults();
			}
		} catch (Exception e) {
			LOG.error("write file[{}] to {}:{} error", path, host, port, e);
		}
		
		return null;
	}
	
	@Override
	public boolean flush(String path) {
		URI uri = new URIBuilder()
		.setScheme(DEFAULT_SCHEME)
		.setHost(host)
		.setPort(port)
		.setPath(DiskContext.URI_FLUSH_NODE_ROOT + path)
		.build();
		
		try {
			LOG.info("flush file[{}] to {}:{}", path, host, port);
			HttpResponse response = client.executePost(uri);
			LOG.debug("flush file[{}] response[{}]", path, response.getStatusCode());
			return response.isReponseOK();
		} catch (Exception e) {
			LOG.error("write file[{}] to {}:{} error", path, host, port, e);
		}
		
		return false;
	}
	
	@Override
	public byte[] readData(String path, long offset) throws IOException {
		return readData(path, offset, Integer.MAX_VALUE);
	}

	@Override
	public byte[] readData(String path, long offset, int size) throws IOException {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_DISK_NODE_ROOT + path)
	    .addParameter("offset", String.valueOf(offset))
	    .addParameter("size", String.valueOf(size))
	    .build();

		byte[] result = null;
		try {
			LOG.info("read file[{}] with offset[{}], size[{}] to {}:{}", path, offset, size, host, port);
			HttpResponse response = client.executeGet(uri);
			LOG.debug("read file[{}] response[{}]", path, response.getStatusCode());
			if(response.isReponseOK()) {
				result = response.getResponseBody();
			}
		} catch (Exception e) {
			LOG.error("read file[{}] with[offset={},size={}] at {}:{} error", path, offset, size, host, port, e);
		}
		
		return result;
	}

	@Override
	public long closeFile(String path) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_DISK_NODE_ROOT + path)
	    .build();
		
		try {
			LOG.info("close file[{}] to {}:{}", path, host, port);
			HttpResponse response = client.executeClose(uri);
			LOG.debug("close file[{}] response[{}]", path, response.getStatusCode());
			if(response.isReponseOK()) {
				return Longs.fromByteArray(response.getResponseBody());
			}
		} catch (Exception e) {
			LOG.error("close file[{}] at {}:{} error", path, host, port, e);
		}
		
		return -1;
	}

	@Override
	public boolean deleteFile(String path, boolean force) {
		URIBuilder builder = new URIBuilder();
		builder.setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_DISK_NODE_ROOT + path);
		
		if(force) {
			builder.addParameter("force");
		}

		try {
			LOG.info("delete file[{}] force[{}] to {}:{}", path, force, host, port);
			HttpResponse response = client.executeDelete(builder.build());
			LOG.debug("delete file[{}] response[{}]", path, response.getStatusCode());
			return response.isReponseOK();
		} catch (Exception e) {
			LOG.error("delete file[{}] at {}:{} error", path, host, port, e);
		}

		return false;
	}

	@Override
	public boolean deleteDir(String path, boolean force, boolean recursive) {
		URIBuilder builder = new URIBuilder();
		builder.setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_DISK_NODE_ROOT + path);
		
		if(force) {
			builder.addParameter("force");
		}
		
		if(recursive) {
			builder.addParameter("recursive");
		}

		try {
			LOG.info("delete dir[{}] force[{}] recursive[{}] to {}:{}", path, force, recursive, host, port);
			HttpResponse response = client.executeDelete(builder.build());
			LOG.debug("delete dir[{}] response[{}]", path, response.getStatusCode());
			return response.isReponseOK();
		} catch (Exception e) {
			LOG.error("delete dir[{}] at {}:{} error", path, host, port, e);
		}

		return false;
	}

	@Override
	public long getFileLength(String path) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_LENGTH_NODE_ROOT + path)
	    .build();
		
		try {
			LOG.info("get length from file[{}] to {}:{}", path, host, port);
			HttpResponse response = client.executeGet(uri);
			LOG.debug("get length from file[{}] response[{}]", path, response.getStatusCode());
			if(response.isReponseOK()) {
				return Longs.fromByteArray(response.getResponseBody());
			}
		} catch (Exception e) {
			LOG.error("get sequences of file[{}] at {}:{} error", path, host, port, e);
		}
		
		return -1;
	}

	@Override
	public void copyFrom(String host, int port, String remotePath, String localPath) throws Exception {
		copyInner(FileCopyMessage.DIRECT_FROM_REMOTE, host, port, remotePath, localPath);
	}
	
	@Override
	public void copyTo(String host, int port, String localPath, String remotePath) throws Exception {
		copyInner(FileCopyMessage.DIRECT_TO_REMOTE, host, port, remotePath, localPath);
	}
	
	private void copyInner(int direct, String host, int port, String remotePath, String localPath) throws Exception {
		FileCopyMessage msg = new FileCopyMessage();
		msg.setDirect(direct);
		msg.setRemoteHost(host);
		msg.setRemotePort(port);
		msg.setRemotePath(remotePath);
		msg.setLocalPath(localPath);
		
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_COPY_NODE_ROOT + "/")
	    .build();
		
		client.executePost(uri, ProtoStuffUtils.serialize(msg));
	}

	@Override
	public boolean recover(String path, long fileLength, List<String> fullSates) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_RECOVER_NODE_ROOT + path)
	    .addParameter("length", String.valueOf(fileLength))
	    .addParameter("fulls", Joiner.on(',').join(fullSates))
	    .build();
		
		try {
			LOG.info("recover file[{}] response[{}] to {}:{}", path, host, port);
			HttpResponse response = client.executePost(uri);
			LOG.debug("recover file[{}] response[{}]", path, response.getStatusCode());
			
			return response.isReponseOK();
		} catch (Exception e) {
			LOG.error("recover file[{}] at {}:{} error", path, host, port, e);
		}
		
		return false;
	}

	@Override
	public List<FileInfo> listFiles(String path, int level) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_LIST_NODE_ROOT + path)
	    .addParameter("level", String.valueOf(level))
	    .build();
		
		try {
			HttpResponse response = client.executeGet(uri);
			
			if(response.isReponseOK()) {
				return JsonUtils.toObject(response.getResponseBody(), new TypeReference<List<FileInfo>>() {
				});
			}
		} catch (Exception e) {
			LOG.error("list files of dir[{}] with level[{}] at {}:{} error", path, level, host, port, e);
		}
		
		return null;
	}
	
	@Override
	public void close() throws IOException {
		client.close();
	}

}
