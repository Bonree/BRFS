package com.bonree.brfs.common.http.jetty;

import org.eclipse.jetty.server.handler.ContextHandler;

public class JettyHttpContextHandler {
	private ContextHandler contextHandler;
	
	public JettyHttpContextHandler(String uriRoot) {
		this(uriRoot, null);
	}

	public JettyHttpContextHandler(String uriRoot, JettyHttpRequestHandler handler) {
		this.contextHandler = new ContextHandler(uriRoot);
		this.contextHandler.setHandler(handler);
	}
	
	public String getContextPath() {
		return contextHandler.getContextPath();
	}
	
	public void setJettyHttpRequestHandler(JettyHttpRequestHandler handler) {
		this.contextHandler.setHandler(handler);
	}
	
	ContextHandler getContextHandler() {
		return contextHandler;
	}
}
