package com.bonree.brfs.duplication.filenode.duplicates;

/**
 * 保存数据副本的磁盘节点选择接口
 * 
 * @author yupeng
 *
 */
public interface DuplicateNodeSelector {
	DuplicateNode[] getDuplicationNodes(int storageId, int nums);
}
