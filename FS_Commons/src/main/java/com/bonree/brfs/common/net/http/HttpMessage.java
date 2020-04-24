package com.bonree.brfs.common.net.http;

import java.util.Map;

public interface HttpMessage {
    //获取url路径
    String getPath();

    //获取参数
    Map<String, String> getParams();

    //获取正文数据
    byte[] getContent();
}
