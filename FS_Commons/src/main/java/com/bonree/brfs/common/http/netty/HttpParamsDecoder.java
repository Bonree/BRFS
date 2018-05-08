package com.bonree.brfs.common.http.netty;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.MemoryAttribute;
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
        params.putAll(decodeFromBody(request));
        
        return params;
    }
	
	/**
	 * 解析Uri中携带的Parameter信息
	 * 
	 * @param uri
	 * @return
	 */
	private static Map<String, String> decodeFromUri(String uri) {
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
	
	/**
	 * 获取Form类型传递的参数
	 * 
	 * @param request
	 * @return
	 */
	private static Map<String, String> decodeFromBody(FullHttpRequest request) {
		Map<String, String> params = new HashMap<String, String>();
		
		HttpPostRequestDecoder decoder = null;
		try {
			decoder = new HttpPostRequestDecoder(new DefaultHttpDataFactory(false), request, CharsetUtil.UTF_8);
			
			List<InterfaceHttpData> postList = decoder.getBodyHttpDatas();
	        for (InterfaceHttpData data : postList) {
	            if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.Attribute) {
	                MemoryAttribute attribute = (MemoryAttribute) data;
	                params.put(attribute.getName(), attribute.getValue());
	            }
	        }
		} catch (Exception ignore){
		} finally {
			if(decoder != null) {
				//必须释放，不然会导致内存泄漏
				decoder.destroy();
			}
		}
		
        return params;
	}
}
