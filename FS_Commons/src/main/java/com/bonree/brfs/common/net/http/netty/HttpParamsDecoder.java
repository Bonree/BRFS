package com.bonree.brfs.common.net.http.netty;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Http请求参数的解析类
 * 
 * @author chen
 *
 */
public class HttpParamsDecoder {
	
	public static Map<String, String> decode(FullHttpRequest request) {
        Map<String, String> params = new HashMap<>();

        params.putAll(decodeFromUri(request.uri()));
        
        return params;
    }
	
	/**
	 * 解析Uri中携带的Parameter信息
	 * 
	 * @param uri
	 * @return
	 */
	public static Map<String, String> decodeFromUri(String uri) {
		Map<String, String> params = new HashMap<String, String>();
		
		QueryStringDecoder decoder = new QueryStringDecoder(uri, CharsetUtil.UTF_8, true);
        Map<String, List<String>> paramMap = decoder.parameters();
        for (Map.Entry<String, List<String>> entry : paramMap.entrySet()) {
            if (entry.getValue() != null && !entry.getValue().isEmpty()) {                    
                params.put(entry.getKey(), entry.getValue().get(0));
            }
        }
        
        return params;
	}
}
