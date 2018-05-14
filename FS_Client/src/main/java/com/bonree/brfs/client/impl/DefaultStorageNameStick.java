package com.bonree.brfs.client.impl;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.bonree.brfs.client.InputItem;
import com.bonree.brfs.client.StorageNameStick;
import com.bonree.brfs.client.route.ServiceMetaInfo;
import com.bonree.brfs.client.route.ServiceSelectorCache;
import com.bonree.brfs.client.utils.FidDecoder;
import com.bonree.brfs.client.utils.FilePathBuilder;
import com.bonree.brfs.common.http.client.HttpClient;
import com.bonree.brfs.common.http.client.HttpResponse;
import com.bonree.brfs.common.http.client.URIBuilder;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.ProtoStuffUtils;
import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.common.write.data.WriteDataMessage;
import com.google.common.base.Joiner;

public class DefaultStorageNameStick implements StorageNameStick {
	private static final String URI_DATA_ROOT = "/duplication/";
	private static final String URI_DISK_NODE_ROOT = "/disk";
	
	private static final String DEFAULT_SCHEME = "http";
	
	private String storageName;
	private int storageId;

	private ServiceSelectorCache selector;
	private HttpClient client = new HttpClient();

	public DefaultStorageNameStick(String storageName, int storageId, ServiceSelectorCache selector) {
		this.storageName = storageName;
		this.storageId = storageId;
		this.selector = selector;
	}

	@Override
	public String[] writeData(InputItem[] itemArrays) {
		Service service = new Service();//.writerService();
		service.setHost("localhost");
		service.setPort(8880);
		
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(service.getHost())
	    .setPort(service.getPort())
	    .setPath(URI_DATA_ROOT)
	    .build();

		try {
			WriteDataMessage dataMessage = new WriteDataMessage();
			dataMessage.setStorageNameId(storageId);

			DataItem[] dataItems = new DataItem[itemArrays.length];
			for (int i = 0; i < dataItems.length; i++) {
				dataItems[i] = new DataItem();
				dataItems[i].setSequence(i);
				dataItems[i].setBytes(itemArrays[i].getBytes());
			}
			dataMessage.setItems(dataItems);
			
			HttpResponse response = client.executePost(uri, ProtoStuffUtils.serialize(dataMessage));
			
			if(response.isReponseOK()) {
				JSONArray array = JSONArray.parseArray(new String(response.getResponseBody()));
				String[] fids = new String[array.size()];
				for(int i = 0; i < array.size(); i++) {
					fids[i] = array.getString(i);
				}
				
				return fids;
			}
		} catch (Exception e) {
			e.printStackTrace();
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
		if(fidObj.getStorageNameCode() != storageId) {
			return null;
		}
		
		List<String> parts = new ArrayList<String>();
		parts.add(fidObj.getUuid());
		for(int serverId : fidObj.getServerIdList()) {
			parts.add(String.valueOf(serverId));
		}
		
		ServiceMetaInfo service = selector.readerService(Joiner.on('_').join(parts));
		
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(service.getFirstServer().getHost())
	    .setPort(service.getFirstServer().getPort())
	    .setPath(URI_DISK_NODE_ROOT + FilePathBuilder.buildPath(fidObj, storageName, service.getReplicatPot()))
	    .addParameter("offset", String.valueOf(fidObj.getOffset()))
	    .addParameter("size", String.valueOf(fidObj.getSize()))
	    .build();

		try {
			HttpResponse response = client.executeGet(uri);
			
			if(response.isReponseOK()) {
				return new InputItem() {
					
					@Override
					public byte[] getBytes() {
						return response.getResponseBody();
					}
				};
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public boolean deleteData(long startTime, long endTime) {
		Service service = selector.randomService();
		
		URI uri = new URIBuilder()
	    .setScheme(DEFAULT_SCHEME)
	    .setHost(service.getHost())
	    .setPort(service.getPort())
	    .setPath(URI_DATA_ROOT)
	    .addParameter("start", String.valueOf(startTime))
	    .addParameter("end", String.valueOf(endTime))
	    .build();
		
		try {
			HttpResponse response = client.executeDelete(uri);
			
			return response.isReponseOK();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}

}
