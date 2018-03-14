package com.br.disknode.server.jetty;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.br.disknode.DiskNodeManager;
import com.br.disknode.DiskReader;
import com.br.disknode.WriteWorker;
import com.br.disknode.watch.WatchListener;
import com.br.disknode.watch.WatchMarket;

public class DiskHandler extends AbstractHandler {
	private static final Logger LOG = LoggerFactory.getLogger(DiskHandler.class);
	
	private DiskNodeManager nodeManager;
	
	public DiskHandler(DiskNodeManager nodeManager) {
		this.nodeManager = nodeManager;
		WatchMarket.get().addListener(WriteWorker.WATCHER_NAME, new WatchListener() {
			
			@Override
			public void watchHappened(List<Object> metrics) {
				for(Object obj : metrics) {
					Map<String, Integer> datas = (Map<String, Integer>) obj;
					for(Entry<String, Integer> item : datas.entrySet()) {
						LOG.info("METRIC [{} : {}]", item.getKey(), item.getValue());
					}
				}
			}
		});
	}

	@Override
	public void handle(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String method = request.getMethod();
		LOG.debug("Handle Request[{} : {}]", method, target);
		
		int status = 0;
		byte[] resBody = null;
		if(method.equals("PUT")) {
			String filePath = target;
			boolean override = contains(request.getParameterNames(), "override");
			LOG.debug("PUT override = {}", override);
			
			try {
				nodeManager.createWriter(filePath, override);
				status = 200;
			} catch (IOException e) {
				LOG.error("create writer", e);
				status = 401;
			}
		} else if(method.equals("POST")) {
			String filePath = target;
			int contentLength = request.getContentLength();
			byte[] buf = new byte[contentLength];
			int offset = 0;
			int len = 0;
			try {
				while (contentLength > 0
						&& (len = request.getInputStream().read(buf, offset,
								contentLength)) > 0) {
					offset += len;
					contentLength -= len;
				}
				LOG.debug("Content length=[{}], read data length = {}",
						request.getContentLength(), offset);
				nodeManager.put(filePath, buf);
				status = 200;
			} catch (InterruptedException e) {
				e.printStackTrace();
				status = 401;
			} catch (IOException e) {
				e.printStackTrace();
				status = 401;
			}
		}else if(method.equals("GET")){
			String filePath = target;
			String offsetParam = request.getParameter("offset");
			String lengthParam = request.getParameter("length");
			int offset = offsetParam == null ? 0 : Integer.parseInt(offsetParam);
			int length = lengthParam == null ? Integer.MAX_VALUE : Integer.parseInt(lengthParam);
			LOG.debug("read file[{}] offset={},length={}", filePath, offset, length);
			
			DiskReader reader = null;
			try {
				reader = new DiskReader(filePath);
				resBody = reader.read(offset, length);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if(reader != null) {
					try {
						reader.close();
					} catch (IOException ignore) {
					}
					
				}
			}
			
			status = 200;
		} else if(method.equals("CLOSE")) {
			try {
				nodeManager.close(target);
				status = 200;
			} catch (Exception e) {
				e.printStackTrace();
				status = 401;
			}
		} else if(method.equals("DELETE")) {
			String filePath = target;
			File file = new File(filePath);
			file.delete();
			status = 200;
		}
		
		baseRequest.setHandled(true);
		response.setContentType("application/json");
		response.setStatus(status);
		if(resBody != null) {
			response.getOutputStream().write(resBody);
		}
	}

	private boolean contains(Enumeration<String> params, String param) {
		while(params.hasMoreElements()) {
			if(params.nextElement().equals(param)) {
				return true;
			}
		}
		
		return false;
	}
}
