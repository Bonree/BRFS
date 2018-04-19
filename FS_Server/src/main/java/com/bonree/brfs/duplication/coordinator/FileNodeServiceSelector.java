package com.bonree.brfs.duplication.coordinator;

import java.util.List;

import com.bonree.brfs.common.service.Service;

public interface FileNodeServiceSelector {
	Service selectWith(FileNode fileNode, List<Service> services);
}
