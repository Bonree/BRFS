package com.bonree.brfs.identification;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年03月31日 11:28:20
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 根据二级serverid获取磁盘路径
 ******************************************************************************/

public interface PartitionInterface {
    /**
     * 根据二级serverid与storageRegionid 获取存储的目录
     *
     * @param secondId
     * @param storageRegionId
     *
     * @return
     */
    String getDataDir(String secondId, int storageRegionId);

    /**
     * 根据文件块名称获取文件块所在的磁盘路径
     *
     * @param fileName
     * @param storageRegionId
     *
     * @return
     */
    String getDataDirByFileName(String fileName, int storageRegionId);

    /**
     * 根据相对路径获取文件块所在的磁盘路径
     *
     * @param filePath
     *
     * @return
     */
    String getDataDirByPath(String filePath);
}
