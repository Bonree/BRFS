package com.bonree.brfs.client.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
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
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import com.alibaba.fastjson.JSONArray;
import com.bonree.brfs.client.InputItem;
import com.bonree.brfs.client.StorageNameStick;
import com.bonree.brfs.client.route.ServiceSelectorCache;
import com.bonree.brfs.client.utils.FidDecoder;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.InputUtils;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.duplication.datastream.handler.DataItem;
import com.bonree.brfs.duplication.datastream.handler.WriteDataMessage;
import com.google.common.base.Joiner;

public class DefaultStorageNameStick implements StorageNameStick {
	private static final String URI_DATA_ROOT = "/duplication/";
	
	private int storageId;

	private ServiceSelectorCache selector;

	public DefaultStorageNameStick(int storageId, ServiceSelectorCache selector) {
		this.storageId = storageId;
		this.selector = selector;
	}

	private URI buildUri(Service service, String root, String path,
			Map<String, String> params) {
		List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
		for (String name : params.keySet()) {
			nameValuePairs.add(new BasicNameValuePair(name, params.get(name)));
		}

		try {
			return new URIBuilder().setScheme("http")
					.setHost(service.getHost())
					.setPort(service.getPort()).setPath(root + path)
					.setParameters(nameValuePairs).build();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public String[] writeData(InputItem[] itemArrays) {
		HttpPost httpPost = new HttpPost(buildUri(selector.writerService(), URI_DATA_ROOT, "", new HashMap<String, String>()));
		WriteDataMessage dataMessage = new WriteDataMessage();
		dataMessage.setStorageNameId(storageId);

		DataItem[] dataItems = new DataItem[itemArrays.length];
		for (int i = 0; i < dataItems.length; i++) {
			dataItems[i] = new DataItem();
			dataItems[i].setSequence(i);
			dataItems[i].setBytes(itemArrays[i].getBytes());
		}
		dataMessage.setItems(dataItems);

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			ByteArrayEntity requestEntity = new ByteArrayEntity(
					ProtoStuffUtils.serialize(dataMessage));
			httpPost.setEntity(requestEntity);

			response = client.execute(httpPost);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() == 200) {
				HttpEntity responseEntity = response.getEntity();
				byte[] resultBytes = new byte[(int) responseEntity.getContentLength()];
				InputUtils.readBytes(responseEntity.getContent(), resultBytes, 0, resultBytes.length);
				
				JSONArray array = JSONArray.parseArray(new String(resultBytes));
				String[] fids = new String[array.size()];
				for(int i = 0; i < array.size(); i++) {
					fids[i] = array.getString(i);
				}
				
				return fids;
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(client);
			CloseUtils.closeQuietly(response);
		}

		return null;
	}

	@Override
	public String writeData(InputItem item) {
		String[] fids = writeData(new InputItem[]{item});
		if(fids != null && fids.length > 0) {
			return fids[0];
		}
		
		return null;
	}

	@Override
	public InputItem readData(String fid) throws Exception {
		Fid fidObj = FidDecoder.build(fid);
		List<String> parts = new ArrayList<String>();
		parts.add(fidObj.getUuid());
		for(int serverId : fidObj.getServerIdList()) {
			parts.add(String.valueOf(serverId));
		}
		HttpGet httpGet = new HttpGet(buildUri(selector.readerService(Joiner.on('_').join(parts)), URI_DATA_ROOT, fid, new HashMap<String, String>()));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpGet);
			StatusLine status = response.getStatusLine();
			if (status.getStatusCode() == 200) {
				HttpEntity entity = response.getEntity();
				byte[] bytes = new byte[(int) entity.getContentLength()];
				InputUtils.readBytes(entity.getContent(), bytes, 0, bytes.length);

				return new SimpleDataItem(bytes);
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(client);
			CloseUtils.closeQuietly(response);
		}
		
		return null;
	}

	@Override
	public boolean deleteData(long startTime, long endTime) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("start", String.valueOf(startTime));
		params.put("end", String.valueOf(endTime));
		HttpDelete httpDelete = new HttpDelete(buildUri(selector.randomService(), URI_DATA_ROOT, "/", new HashMap<String, String>()));

		CloseableHttpClient client = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(httpDelete);
			StatusLine status = response.getStatusLine();
			return status.getStatusCode() == 200;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CloseUtils.closeQuietly(client);
			CloseUtils.closeQuietly(response);
		}
		
		return false;
	}

}
