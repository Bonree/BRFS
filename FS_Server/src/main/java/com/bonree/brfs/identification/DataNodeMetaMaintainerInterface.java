package com.bonree.brfs.identification;

import com.bonree.brfs.common.resource.vo.DataNodeMetaModel;
import java.util.Collection;

/**
 * datanode 元数据维护接口，负责获取元数据信息，将元数据信息更新到存储介质
 */
public interface DataNodeMetaMaintainerInterface {
    /**
     * 从存储介质获取当前datanode节点的元信息，若信息不存在则返回null
     *
     * @return
     */
    DataNodeMetaModel getDataNodeMeta() throws Exception;

    /**
     * 将datanode信息更新到存储介质，
     *
     * @param metaData
     */
    void updateDataNodeMeta(DataNodeMetaModel metaData) throws Exception;

    Collection<String> getExistFirst() throws Exception;
}
