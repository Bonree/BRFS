package com.bonree.brfs.duplication.datastream.connection;

import java.io.Closeable;

import com.bonree.brfs.disknode.client.DiskNodeClient;

public interface DiskNodeConnection extends Closeable {
	String getRemoteAddress();
	int getRemotePort();
	boolean isValid();
	DiskNodeClient getClient();
}
