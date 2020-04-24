package com.bonree.brfs.common.net.http.client;

/**
 * Http响应处理接口
 *
 * @author yupeng
 */
public interface ResponseHandler {
    /**
     * 收到Http响应
     *
     * @param response
     */
    void onCompleted(HttpResponse response);

    /**
     * Http请求处理异常
     *
     * @param e
     */
    void onThrowable(Throwable e);
}
