package com.bonree.brfs.duplication.datastream.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.asynctask.AsyncTask;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;

public class DataWriteTask extends AsyncTask<WriteResult[]> {
	private static final Logger LOG = LoggerFactory.getLogger(DataWriteTask.class);
	
	private DiskNodeConnection connection;
	private String filePath;
	private WriteData[] datas;
	
	public DataWriteTask(String path, DiskNodeConnection connection, WriteData[] datas) {
		this.connection = connection;
		this.filePath = path;
		this.datas = datas;
	}

	@Override
	public WriteResult[] run() throws Exception {
		long start = System.currentTimeMillis();
		try {
			if(connection == null) {
				return null;
			}
			
			DiskNodeClient client = connection.getClient();
			if(client == null) {
				return null;
			}
			
			LOG.info("write {} data to {}:{}", filePath, connection.getService().getHost(), connection.getService().getPort());
			WriteResult[] result = client.writeDatas(filePath, datas);
			
			LOG.info("get result---" + result);
			
			return result;
		} finally {
			System.out.println("take##############" + (System.currentTimeMillis() - start));
		}
	}
	
}
