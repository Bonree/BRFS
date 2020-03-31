package com.bonree.brfs.duplication.rocksdb;

import com.bonree.brfs.common.process.LifeCycle;
import org.rocksdb.RocksDB;

import java.util.List;
import java.util.Map;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 16:35
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: RocksDB相关操作接口
 ******************************************************************************/
public interface RocksDBManager extends LifeCycle {

    /**
     * @param columnFamily 列族名称，对应到SN名称
     * @param key          key
     * @return value       返回null则表示key不存在或异常，默认本节点读取不到时去其他节点尝试读取
     * @description: 从RocksDB中获取列族为columnFamily的key值
     */
    byte[] read(String columnFamily, byte[] key);

    /**
     * @param columnFamily  列族名称，对应到SN名称
     * @param key           key
     * @param readFormOther 本节点读取不到时是否去其他节点读取
     * @return value       返回null则表示key不存在或异常
     * @description: 从RocksDB中获取列族为columnFamily的key值
     */
    byte[] read(String columnFamily, byte[] key, boolean readFormOther);

    /**
     * @param columnFamily 列族名称，对应到SN名称
     * @param prefixKey    key前缀
     * @return value       返回null则异常
     * @description: 从RocksDB中获取列族为columnFamily的前缀为prefixKey的所有值
     */
    List<byte[]> readByPrefix(String columnFamily, byte[] prefixKey);

    /**
     * @param columnFamily 列族名称
     * @param key          K
     * @param value        V
     * @return 写入状态 SUCCESS 成功； FAILED 失败
     * @description: 写入RocksDB，若K在RocksDB中已存在，默认覆盖
     */
    WriteStatus write(String columnFamily, byte[] key, byte[] value) throws Exception;

    /**
     * @param columnFamily 列族名称
     * @param key          K
     * @param value        V
     * @param force        若RocksDB中已存在该KEY，是否强制覆盖，true:覆盖；false：不覆盖
     * @return 写入状态 SUCCESS 成功； FAILED 失败 ； KEY_EXISTS key已存在
     * @description: 写入RocksDB
     */
    WriteStatus write(String columnFamily, byte[] key, byte[] value, boolean force) throws Exception;

    /**
     * @param dataUnit 数据单元
     * @return 写入状态
     * @description: 该接口主要供其他RocksDB节点同步数据时使用，正常写入不可用该接口
     */
    WriteStatus write(RocksDBDataUnit dataUnit) throws Exception;

    /**
     * @param columnFamily 列族名称
     * @param ttl          该列族下数据的过期时间，单位秒，如果ttl值为0或者负数则永不过期
     * @description: 创建列族
     */
    void createColumnFamilyWithTtl(String columnFamily, int ttl) throws Exception;

    /**
     * @param columnFamily 列族名称
     * @description: 删除列族
     */
    void deleteColumnFamily(String columnFamily) throws Exception;

    /**
     * @param columnFamilyMap ZK上的列族信息
     * @description: 更新本地列族信息缓存
     */
    void updateColumnFamilyHandles(Map<String, Integer> columnFamilyMap) throws Exception;

    /**
     * @param srcPath 其他RocksDB数据目录
     * @description: 将其他数据目录的数据迁移到当前数据目录中
     */
    void dataTransfer(String srcPath);

    /**
     * @description: 获取RocksDB连接对象
     */
    RocksDB getRocksDB();
}
