package com.bonree.brfs.rocksdb.handler;

import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.HttpMessage;
import com.bonree.brfs.common.net.http.MessageHandler;
import io.netty.channel.ChannelHandler;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 14:16
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/

@ChannelHandler.Sharable
public class PingRequestHandler implements MessageHandler {

    @Override
    public boolean isValidRequest(HttpMessage message) {
        return true;
    }

    @Override
    public void handle(HttpMessage msg, HandleResultCallback callback) {
        callback.completed(new HandleResult(true));
    }

}
