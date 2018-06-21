package com.bonree.brfs.common.net.http.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;

public class HttpResponseProxy implements HttpResponse {
	private org.apache.http.HttpResponse delegate;
	
	public HttpResponseProxy(org.apache.http.HttpResponse delegate) {
		this.delegate = delegate;
	}
	
	@Override
	public boolean isReponseOK() {
		return delegate.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
	}
	
	@Override
	public String getProtocolVersion() {
		return delegate.getStatusLine().getProtocolVersion().toString();
	}

	@Override
	public int getStatusCode() {
		return delegate.getStatusLine().getStatusCode();
	}

	@Override
	public String getStatusText() {
		return delegate.getStatusLine().getReasonPhrase();
	}

	@Override
	public byte[] getResponseBody() {
		HttpEntity entity = delegate.getEntity();
		if(entity == null) {
			return new byte[0];
		}
		
		int length = (int) (entity.getContentLength() > 0 ? entity.getContentLength() : 0);
		ByteArrayOutputStream byteArray = new ByteArrayOutputStream(length);
		try {
			entity.writeTo(byteArray);
			return byteArray.toByteArray();
		} catch (IOException e) {
		}
		
		return new byte[0];
	}

}
