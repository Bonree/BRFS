package com.bonree.brfs.disknode.client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.InputUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.disknode.server.handler.data.FileCopyMessage;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.google.common.io.Closeables;

public class HttpDiskNodeClient implements DiskNodeClient {
	private static final Logger LOG = LoggerFactory.getLogger(HttpDiskNodeClient.class);

	private static final String URI_DISK_NODE_ROOT = "/disk";
	private static final String URI_INFO_NODE_ROOT = "/info";
	private static final String URI_COPY_NODE_ROOT = "/copy";
	private static final String URI_LIST_NODE_ROOT = "/list";

	private static final int STATUS_OK = 200;

	private String host;
	private int port;

	public HttpDiskNodeClient(String host, int port) {
		this.host = host;
		this.port = port;
	}

	private URI buildUri(String root, String path, Map<String, String> params) {
		List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
		for(String name : params.keySet()) {
			nameValuePairs.add(new BasicNameValuePair(name, params.get(name)));
		}
		
		try {
			return new URIBuilder()
			.setScheme("http")
			.setHost(host)
			.setPort(port)
			.setPath(root + path)
			.setParameters(nameValuePairs)
			.build();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public boolean initFile(String path) {
		Map<String, String> params = new HashMap<String, String>();
		HttpPut httpPut = new HttpPut(buildUri(URI_DISK_NODE_ROOT, path, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpPut);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == STATUS_OK;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				Closeables.close(response, true);
				Closeables.close(client, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	@Override
	public WriteResult writeData(String path, int sequence, byte[] bytes) throws IOException {
		return writeData(path, sequence, bytes, 0, bytes.length);
	}

	@Override
	public WriteResult writeData(String path, int sequence, byte[] bytes, int offset, int size)
			throws IOException {
		HttpPost httpPost = new HttpPost(buildUri(URI_DISK_NODE_ROOT, path, new HashMap<String, String>()));
		WriteData writeItem = new WriteData();
		writeItem.setSequence(sequence);
		//TODO warning: maybe a defect of performance! because of coping of byte arrays!
		writeItem.setBytes(Arrays.copyOfRange(bytes, offset, offset + size));
		ByteArrayEntity requestEntity = new ByteArrayEntity(ProtoStuffUtils.serialize(writeItem));
		httpPost.setEntity(requestEntity);

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpPost);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() != STATUS_OK) {
				throw new IOException("status = " + status.getStatusCode());
			}

			HttpEntity responseEntity = response.getEntity();
			byte[] resultBytes = new byte[(int) responseEntity.getContentLength()];
			InputUtils.readBytes(responseEntity.getContent(), resultBytes, 0,
					resultBytes.length);

			return ProtoStuffUtils.deserialize(resultBytes, WriteResult.class);
		} catch (ClientProtocolException e) {
			throw new IOException(e);
		} finally {
			try {
				Closeables.close(response, true);
				Closeables.close(client, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public byte[] readData(String path, int offset, int size)
			throws IOException {
		Map<String, String> params = new HashMap<String, String>();
		params.put("offset", Integer.toString(offset));
		params.put("size", Integer.toString(size));
		HttpGet httpGet = new HttpGet(buildUri(URI_DISK_NODE_ROOT, path, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpGet);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() != STATUS_OK) {
				throw new IOException();
			}

			HttpEntity entity = response.getEntity();
			byte[] bytes = new byte[(int) entity.getContentLength()];
			InputUtils.readBytes(entity.getContent(), bytes, 0, bytes.length);

			return bytes;
		} catch (ClientProtocolException e) {
			throw new IOException(e);
		} finally {
			try {
				Closeables.close(response, true);
				Closeables.close(client, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public boolean closeFile(String path) {
		HttpClose httpClose = new HttpClose(buildUri(URI_DISK_NODE_ROOT, path, new HashMap<String, String>()));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpClose);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == STATUS_OK;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				Closeables.close(response, true);
				Closeables.close(client, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	@Override
	public boolean deleteFile(String path, boolean force) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("force", Boolean.toString(force));
		params.put("recursive", Boolean.toString(false));
		HttpDelete httpDelete = new HttpDelete(buildUri(URI_DISK_NODE_ROOT, path, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpDelete);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == STATUS_OK;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				Closeables.close(response, true);
				Closeables.close(client, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	@Override
	public boolean deleteDir(String path, boolean force, boolean recursive) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("force", Boolean.toString(force));
		params.put("recursive", Boolean.toString(recursive));
		HttpDelete httpDelete = new HttpDelete(buildUri(URI_DISK_NODE_ROOT, path, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpDelete);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == STATUS_OK;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				Closeables.close(response, true);
				Closeables.close(client, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	@Override
	public BitSet getWritingSequence(String path) {
		Map<String, String> params = new HashMap<String, String>();
		HttpGet httpGet = new HttpGet(buildUri(URI_INFO_NODE_ROOT, path, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpGet);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() == STATUS_OK) {
				HttpEntity entity = response.getEntity();
				byte[] bytes = new byte[(int) entity.getContentLength()];
				InputUtils.readBytes(entity.getContent(), bytes, 0, bytes.length);

				return BitSet.valueOf(bytes);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				Closeables.close(response, true);
				Closeables.close(client, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}

	@Override
	public void copyFrom(String host, int port, String remotePath, String localPath) throws IOException {
		copyInner(FileCopyMessage.DIRECT_FROM_REMOTE, host, port, remotePath, localPath);
	}
	
	@Override
	public void copyTo(String host, int port, String localPath, String remotePath) throws IOException {
		copyInner(FileCopyMessage.DIRECT_TO_REMOTE, host, port, remotePath, localPath);
	}
	
	private void copyInner(int direct, String host, int port, String remotePath, String localPath) throws IOException {
		Map<String, String> params = new HashMap<String, String>();
		HttpPost httpPost = new HttpPost(buildUri(URI_COPY_NODE_ROOT, "/", params));
		FileCopyMessage msg = new FileCopyMessage();
		msg.setDirect(direct);
		msg.setRemoteHost(host);
		msg.setRemotePort(port);
		msg.setRemotePath(remotePath);
		msg.setLocalPath(localPath);
		
		ByteArrayEntity requestEntity = new ByteArrayEntity(ProtoStuffUtils.serialize(msg));
		httpPost.setEntity(requestEntity);

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpPost);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() != STATUS_OK) {
				throw new IOException("copy failed!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(response);
			CloseUtils.closeQuietly(client);
		}
	}

	@Override
	public void recover(String path, SeqInfoList infos) throws IOException {
		Map<String, String> params = new HashMap<String, String>();
		HttpPost httpPost = new HttpPost(buildUri(URI_INFO_NODE_ROOT, path, params));
		ByteArrayEntity requestEntity = new ByteArrayEntity(ProtoStuffUtils.serialize(infos));
		httpPost.setEntity(requestEntity);

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpPost);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() == STATUS_OK) {
				HttpEntity entity = response.getEntity();
				byte[] bytes = new byte[(int) entity.getContentLength()];
				InputUtils.readBytes(entity.getContent(), bytes, 0, bytes.length);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(response);
			CloseUtils.closeQuietly(client);
		}
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] getBytesBySequence(String path, int sequence) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("seq", String.valueOf(sequence));
		HttpGet httpGet = new HttpGet(buildUri(URI_INFO_NODE_ROOT, path, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpGet);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() == STATUS_OK) {
				HttpEntity entity = response.getEntity();
				byte[] bytes = new byte[(int) entity.getContentLength()];
				InputUtils.readBytes(entity.getContent(), bytes, 0, bytes.length);

				return bytes;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				Closeables.close(response, true);
				Closeables.close(client, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}

	@Override
	public List<FileInfo> listFiles(String path, int level) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("level", String.valueOf(level));
		HttpGet httpGet = new HttpGet(buildUri(URI_LIST_NODE_ROOT, path, params));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpGet);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() == STATUS_OK) {
				HttpEntity entity = response.getEntity();
				byte[] bytes = new byte[(int) entity.getContentLength()];
				InputUtils.readBytes(entity.getContent(), bytes, 0, bytes.length);

				JSONArray array = JSONArray.parseArray(BrStringUtils.fromUtf8Bytes(bytes));
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
		} finally {
			try {
				Closeables.close(response, true);
				Closeables.close(client, true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}

}
