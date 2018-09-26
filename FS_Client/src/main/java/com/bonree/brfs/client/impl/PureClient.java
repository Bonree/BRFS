package com.bonree.brfs.client.impl;

import java.io.IOException;

import com.bonree.brfs.client.utils.FilePathBuilder;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.write.data.FidDecoder;

public class PureClient {
	private ReadConnectionPool connectionPool;
	private ReadObject readObject;
	
	private Service service;
	
	public PureClient(String fid, String ip, int port, String sr, int index) throws Exception {
		this.service = new Service("id", "gp", ip, port);
		this.service.setExtraPort(port);
		
		Fid fidObj = FidDecoder.build(fid);

        readObject = new ReadObject();
    	readObject.setFilePath(FilePathBuilder.buildPath(fidObj,
    			sr, index));
    	readObject.setOffset(fidObj.getOffset());
    	readObject.setLength((int) fidObj.getSize());
		
		this.connectionPool = new ReadConnectionPool();
	}
	
	public byte[] read() throws Exception {
		ReadConnection client = connectionPool.getConnection(service);
		
		return client.read(readObject);
	}
	
	public void close() throws IOException {
	}
}
