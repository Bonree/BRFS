package com.bonree.brfs.common.http.client;

/**
 * http响应数据
 * 
 * @author yupeng
 *
 */
public interface HttpResponse {
	boolean isReponseOK();
	String getProtocolVersion();
	int getStatusCode();
	String getStatusText();
	byte[] getResponseBody();
}
