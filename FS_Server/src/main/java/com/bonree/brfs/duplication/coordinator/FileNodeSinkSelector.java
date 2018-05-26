package com.bonree.brfs.duplication.coordinator;

import java.util.List;

import com.bonree.brfs.common.service.Service;

/**
 * 为需要转移的文件选择一个合适服务
 * 
 * @author yupeng
 *
 */
public interface FileNodeSinkSelector {
	/**
	 * 为{@link FileNode}选择一个合适的sink进行转移
	 * 
	 * @param fileNode
	 * @param services
	 * @return
	 */
	Service selectWith(FileNode fileNode, List<Service> serviceList);
}
