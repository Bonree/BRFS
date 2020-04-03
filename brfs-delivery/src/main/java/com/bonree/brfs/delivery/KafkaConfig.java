package com.bonree.brfs.delivery;

import com.bonree.brfs.configuration.ConfigUnit;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date: 19-2-27下午6:10
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description:
 ******************************************************************************/
public class KafkaConfig {

    /**
     * kafka brokers
     */
    public static final ConfigUnit<String> CONFIG_BROKERS =
            ConfigUnit.ofString("kafka.brokers", "127.0.0.1:9092");

    public static final ConfigUnit<String> CONFIG_TOPIC =
            ConfigUnit.ofString("kafka.topic","brfs_merit");

    public static final ConfigUnit<Integer> CONFIG_QUEUE_SIZE =
            ConfigUnit.ofInt("deliver.queue.size", 200000);

    public static final ConfigUnit<Boolean> CONFIG_DELIVER_SWITCH =
            ConfigUnit.ofBoolean("deliver.switch", false);

    public static final ConfigUnit<String> CONFIG_META_URL =
            ConfigUnit.ofString("deliver.meta.url", "http://devtest.ibr.cc:20003/v1");

    public static final ConfigUnit<String> CONFIG_DATA_SOURCE =
            ConfigUnit.ofString("deliver.datasource", "sdk_data_brfs");

    public static final ConfigUnit<String> CONFIG_READER_TABLE =
            ConfigUnit.ofString("deliver.table.reader", "brfs_reader_metric");

    public static final ConfigUnit<String> CONFIG_WRITER_TABLE =
            ConfigUnit.ofString("deliver.table.writer", "brfs_writer_metric");


}
