package com.bonree.brfs.disknode.server.jetty;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.http.HandleResult;
import com.bonree.brfs.common.http.HandleResultCallback;
import com.bonree.brfs.common.http.MessageHandler;
import com.bonree.brfs.common.utils.InputUtils;
import com.bonree.brfs.disknode.server.DiskMessage;

public class DiskJettyHttpRequestHandler extends AbstractHandler {
	private static final Logger LOG = LoggerFactory.getLogger(DiskJettyHttpRequestHandler.class);
	
	private Map<String, MessageHandler<DiskMessage>> methodToOps = new HashMap<String, MessageHandler<DiskMessage>>();
	
	public void put(String method, MessageHandler<DiskMessage> handler) {
		methodToOps.put(method, handler);
	}

	@Override
	public void handle(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		LOG.debug("handle request[{}:{}]", request.getMethod(), target);
		
		MessageHandler<DiskMessage> handler = methodToOps.get(request.getMethod());
		if(handler == null) {
			baseRequest.setHandled(true);
			responseError(response, HttpStatus.Code.METHOD_NOT_ALLOWED);
			return;
		}
		
		DiskMessage message = new DiskMessage();
		message.setFilePath(target);
		
		int contentLength = request.getContentLength();
		System.out.println("content length############" + contentLength);
		byte[] data = new byte[Math.max(contentLength, 0)];
		if(request.getContentLength() > 0) {
			InputUtils.readBytes(request.getInputStream(), data, 0, data.length);
			
			System.out.println(new String(data));
		}
		message.setData(data);
		
		Map<String, String> params = new HashMap<String, String>();
		for(String paramName : request.getParameterMap().keySet()) {
			params.put(paramName, request.getParameter(paramName));
			System.out.println(paramName + "---" + request.getParameter(paramName));
		}
		message.setParams(params);
		
		baseRequest.setHandled(true);
		
		//采用异步方式处理Http响应
		AsyncContext context = request.startAsync();
		System.out.println("########################START HANDLING##########################");
		handler.handle(message, new DefaultHandleResultCallback(context));
	}
	
	private void responseError(HttpServletResponse response, HttpStatus.Code status) throws IOException {
		response.setStatus(status.getCode());
		response.getWriter().write("Failure: " + status.getMessage() + "\r\n");
		response.setContentType("text/plain; charset=UTF-8");
	}

	private class DefaultHandleResultCallback implements HandleResultCallback {
		private AsyncContext context;
		
		public DefaultHandleResultCallback(AsyncContext context) {
			this.context = context;
		}

		@Override
		public void completed(HandleResult result) {
			HttpStatus.Code status = HttpStatus.Code.OK;
			byte[] content = new byte[0];
			if(!result.isSuccess()) {
				status = HttpStatus.Code.INTERNAL_SERVER_ERROR;
				if(result.getCause() != null) {
					content = result.getCause().toString().getBytes(Charset.forName("UTF-8"));
				}
			} else if(result.getData() != null) {
				content = result.getData();
			}
			
			HttpServletResponse response = (HttpServletResponse) context.getResponse();
			response.setStatus(status.getCode());
			try {
				response.getOutputStream().write(content);
				response.setContentType("text/plain");
				response.setContentLength(content.length);
			} catch (Exception e) {
			}
			
			context.complete();
		}
		
	}
}
