package com.bonree.brfs.duplication.datastream.connection;

import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

public interface DiskNodeConnectionPool{
	DiskNodeConnection getConnection(DuplicateNode duplicateNode);
}
