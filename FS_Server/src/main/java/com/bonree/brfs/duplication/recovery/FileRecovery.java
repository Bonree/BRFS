package com.bonree.brfs.duplication.recovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.utils.LifeCycle;

public class FileRecovery implements LifeCycle{
	
	private Map<Integer, DiskNodeClient> clientCaches = new HashMap<Integer, DiskNodeClient>();
	
	private List<FileNode> recoveringFileNodes = new ArrayList<FileNode>();

	public void recover(FileNode fileNode) {
		recoveringFileNodes.add(fileNode);
	}

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		
	}
}
