package com.bonree.brfs.kafka;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date: 19-3-1下午4:56
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description:
 ******************************************************************************/
public class ProducerClientTest {

    public static void main(String[] args) {
        ProducerClient.getInstance();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            ProducerClient.getInstance().sendMessage(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        }
        long end = System.currentTimeMillis();
        System.out.println("use time:" + (end - begin) + "ms");

    }
}
