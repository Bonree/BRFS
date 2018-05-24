package com.bonree.brfs.duplication.datastream.connection;

import java.io.Closeable;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.disknode.client.DiskNodeClient;

public interface DiskNodeConnection extends Closeable {
	Service getService();
	boolean isValid();
	DiskNodeClient getClient();
}
