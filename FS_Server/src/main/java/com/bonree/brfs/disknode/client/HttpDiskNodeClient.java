package com.bonree.brfs.disknode.client;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.common.http.client.ClientConfig;
import com.bonree.brfs.common.http.client.HttpClient;
import com.bonree.brfs.common.http.client.HttpResponse;
import com.bonree.brfs.common.http.client.URIBuilder;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.disknode.DiskContext;
import com.bonree.brfs.disknode.server.handler.data.FileCopyMessage;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;

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
		}
		
		return false;
	}

	@Override
	public WriteResult writeData(String path, int sequence, byte[] bytes) throws IOException {
		WriteData writeItem = new WriteData();
		writeItem.setDiskSequence(sequence);
		writeItem.setBytes(bytes);
		
		WriteResult[] results = writeDatas(path, new WriteData[] {writeItem});
		
		return results != null ? results[0] : null;
	}

	@Override
	public WriteResult writeData(String path, int sequence, byte[] bytes, int offset, int size)
			throws IOException {
		int length = Math.min(size, bytes.length - offset);
		byte[] copy = new byte[length];
		System.arraycopy(bytes, offset, copy, 0, length);
		
		return writeData(path, sequence, copy);
	}
	
	@Override
	public WriteResult[] writeDatas(String path, WriteData[] dataList) throws IOException {
		WriteDataList datas = new WriteDataList();
		datas.setDatas(dataList);
		
		URI uri = new URIBuilder()
		.setScheme(DEFAULT_SCHEME)
		.setHost(host)
		.setPort(port)
		.setPath(DiskContext.URI_DISK_NODE_ROOT + path)
		.build();
		
		try {
			HttpResponse response = client.executePost(uri, ProtoStuffUtils.serialize(datas));
			if(response.isReponseOK()) {
				WriteResultList resultList = ProtoStuffUtils.deserialize(response.getResponseBody(), WriteResultList.class);
				return resultList.getWriteResults();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public byte[] readData(String path, int offset, int size)
			throws IOException {
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
			HttpResponse response = client.executeGet(uri);
			if(response.isReponseOK()) {
				result = response.getResponseBody();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}

	@Override
	public boolean closeFile(String path) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_DISK_NODE_ROOT + path)
	    .build();
		
		try {
			HttpResponse response = client.executeClose(uri);
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
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
			HttpResponse response = client.executeDelete(builder.build());
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
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
			HttpResponse response = client.executeDelete(builder.build());
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public BitSet getWritingSequence(String path) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_INFO_NODE_ROOT + path)
	    .build();
		
		try {
			HttpResponse response = client.executeGet(uri);
			if(response.isReponseOK()) {
				return BitSet.valueOf(response.getResponseBody());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
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
	public boolean recover(String path, RecoverInfo infos) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_RECOVER_NODE_ROOT + path)
	    .build();
		
		try {
			HttpResponse response = client.executePost(uri, ProtoStuffUtils.serialize(infos));
			
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public byte[] getBytesBySequence(String path, int sequence) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_INFO_NODE_ROOT + path)
	    .addParameter("seq", String.valueOf(sequence))
	    .build();
		
		try {
			HttpResponse response = client.executeGet(uri);
			if(response.isReponseOK()) {
				return response.getResponseBody();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
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
				JSONArray array = JSONArray.parseArray(BrStringUtils.fromUtf8Bytes(response.getResponseBody()));
				ArrayList<FileInfo> result = new ArrayList<FileInfo>();
				for(int i = 0; i < array.size(); i++) {
					JSONObject object = array.getJSONObject(i);
					FileInfo info = new FileInfo();
					info.setType(object.getIntValue("type"));
					info.setLevel(object.getIntValue("level"));
					info.setPath(object.getString("path"));
					result.add(info);
				}
				
				return result;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public int[] getWritingFileMetaInfo(String path) {
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(host)
	    .setPort(port)
	    .setPath(DiskContext.URI_META_NODE_ROOT + path)
	    .build();
		
		try {
			HttpResponse response = client.executeGet(uri);
			
			if(response.isReponseOK()) {
				JSONObject json = JSONObject.parseObject(BrStringUtils.fromUtf8Bytes(response.getResponseBody()));
				int[] result = new int[2];
				result[0] = json.getIntValue("seq");
				result[1] = json.getIntValue("length");
				
				return result;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	@Override
	public void close() throws IOException {
		client.close();
	}

}
