package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.common.timer.TimeExchangeEventEmitter;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSynchronizer;
import com.bonree.brfs.duplication.filenode.FileNodeSinkManager;
import com.bonree.brfs.duplication.storageregion.StorageRegion;

public class DefaultFileObjectSupplierFactory implements FileObjectSupplierFactory {
	private FileObjectFactory fileFactory;
	private FileObjectCloser fileCloser;
	private FileObjectSynchronizer fileSynchronizer;
	private FileNodeSinkManager fileNodeSinkManager;
	private TimeExchangeEventEmitter timeEventEmitter;
	
	public DefaultFileObjectSupplierFactory(FileObjectFactory fileFactory,
			FileObjectCloser fileCloser,
			FileObjectSynchronizer fileSynchronizer,
			FileNodeSinkManager fileNodeSinkManager,
			TimeExchangeEventEmitter timeEventEmitter) {
		this.fileFactory = fileFactory;
		this.fileCloser = fileCloser;
		this.fileSynchronizer = fileSynchronizer;
		this.fileNodeSinkManager = fileNodeSinkManager;
		this.timeEventEmitter = timeEventEmitter;
	}

	@Override
	public FileObjectSupplier create(StorageRegion storageRegion) {
		return new DefaultFileObjectSupplier(storageRegion, fileFactory, fileCloser, fileSynchronizer, fileNodeSinkManager, timeEventEmitter);
	}

}
