package com.bonree.brfs.duplication.filenode.zk;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FileNodeSinkSelector;
import java.util.List;
import java.util.Random;

/**
 * 随机选择
 *
 * @author yupeng
 */
public class RandomFileNodeSinkSelector implements FileNodeSinkSelector {
    private static Random random = new Random();

    @Override
    public Service selectWith(FileNode fileNode, List<Service> serviceList) {
        if (serviceList.isEmpty()) {
            return null;
        }

        return serviceList.get(random.nextInt(serviceList.size()));
    }

}
