package com.bonree.brfs.configuration.units;

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
}
