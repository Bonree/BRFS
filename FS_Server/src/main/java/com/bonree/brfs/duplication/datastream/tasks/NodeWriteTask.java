package com.bonree.brfs.duplication.datastream.tasks;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.asynctask.AsyncTask;
import com.bonree.brfs.common.timer.TimeCounter;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.server.handler.data.WriteData;
import com.bonree.brfs.disknode.server.handler.data.WriteResult;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;

public class NodeWriteTask extends AsyncTask<WriteResult[]> {
	private static final Logger LOG = LoggerFactory.getLogger(NodeWriteTask.class);
	
	private DiskNodeConnection connection;
	private String filePath;
	private WriteData[] datas;
	
	public NodeWriteTask(String path, DiskNodeConnection connection, WriteData[] datas) {
		this.connection = connection;
		this.filePath = path;
		this.datas = datas;
	}

	@Override
	public WriteResult[] run() throws Exception {
		TimeCounter counter = new TimeCounter("NodeWriteTask", TimeUnit.MILLISECONDS);
		try {
			counter.begin();
			if(connection == null) {
				LOG.error("file[{}] connection is null!!!", filePath);
				return null;
			}
			
			DiskNodeClient client = connection.getClient();
			if(client == null) {
				LOG.error("file[{}] DiskNodeClient is null!!!", filePath);
				return null;
			}
			
			LOG.info("write {} data to {}:{}", filePath, connection.getRemoteAddress(), connection.getRemotePort());
			WriteResult[] result = client.writeDatas(filePath, datas);
			
			LOG.info("file[{}] write task from {}:{} get result---{}", filePath,
					connection.getRemoteAddress(), connection.getRemotePort(), result);
			
			return result;
		} finally {
			LOG.info(counter.report(0));
		}
	}
	
}
