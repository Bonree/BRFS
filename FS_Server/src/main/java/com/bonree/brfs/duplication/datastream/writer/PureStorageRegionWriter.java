package com.bonree.brfs.duplication.datastream.writer;

import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.duplication.datastream.dataengine.DataEngineManager;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 转换成一期的数据格式
 * 用来把数组写入dn
 *
 * @author wangchao
 */
public class PureStorageRegionWriter extends DefaultStorageRegionWriter {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStorageRegionWriter.class);

    public PureStorageRegionWriter(DataEngineManager dataEngineManager) {
        super(dataEngineManager);
    }

    public void write(int storageRegionId, byte[] data, StorageRegionWriteCallback callback) {
        Preconditions.checkNotNull(data, "写入的数据不应该是空！");
        DataItem dataItem = new DataItem();
        dataItem.setBytes(data);
        write(storageRegionId, new DataItem[] {dataItem}, callback);
    }

}
