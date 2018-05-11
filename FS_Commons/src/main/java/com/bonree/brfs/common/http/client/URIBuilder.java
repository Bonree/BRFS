package com.bonree.brfs.common.http.client;

import java.net.URI;
import java.net.URISyntaxException;

public class URIBuilder {
	private org.apache.http.client.utils.URIBuilder builder;
	
	public URIBuilder setScheme(String scheme) {
		builder.setScheme(scheme);
		return this;
	}
	
	public URIBuilder setHost(String host) {
		builder.setHost(host);
		return this;
	}
	
	public URIBuilder setPort(int port) {
		builder.setPort(port);
		return this;
	}
	
	public URIBuilder setPath(String path) {
		builder.setPath(path);
		return this;
	}
	
	public URIBuilder addParameter(String name, String value) {
		builder.addParameter(name, name);
		return this;
	}
	
	public URIBuilder addParameter(String name) {
		builder.addParameter(name, null);
		return this;
	}
	
	public URIBuilder setParamter(String name, String value) {
		builder.setParameter(name, value);
		return this;
	}
	
	public URI build() {
		try {
			return builder.build();
		} catch (URISyntaxException ignore) {}
		
		return null;
	}
}
