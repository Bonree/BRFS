package com.bonree.brfs.duplication.coordinator;

public class FileCoordinator {
	
	public static final String DUPLICATE_SERVICE_GROUP = "duplicate_group";
	
	private FileNodeStorer storer;
	private FileNodeSinkManager sinkManager;
	
	public FileCoordinator(FileNodeStorer storer, FileNodeSinkManager sinkManager) {
		this.storer = storer;
		this.sinkManager = sinkManager;
	}
	
	public void store(FileNode fileNode) throws Exception {
		storer.save(fileNode);
	}
	
	public void delete(FileNode fileNode) throws Exception {
		storer.delete(fileNode.getName());
	}
	
	public void addFileNodeSink(FileNodeSink sink) throws Exception {
		sinkManager.registerFileNodeSink(sink);
	}
}
