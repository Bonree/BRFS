package com.bonree.brfs.duplication.datastream.connection;

import com.bonree.brfs.common.service.Service;

public interface DiskNodeConnectionFactory {
	DiskNodeConnection createConnection(Service service);
}
