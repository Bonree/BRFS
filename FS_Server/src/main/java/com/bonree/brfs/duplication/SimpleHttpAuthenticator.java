package com.bonree.brfs.duplication;

import com.bonree.brfs.authentication.SimpleAuthentication;
import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.http.netty.HttpAuthenticator;

public class SimpleHttpAuthenticator implements HttpAuthenticator {
	private SimpleAuthentication simpleAuthentication;
	
	public SimpleHttpAuthenticator(SimpleAuthentication simpleAuthentication) {
		this.simpleAuthentication = simpleAuthentication;
	}

	@Override
	public int check(String userName, String passwd) {
		StringBuilder tokenBuilder = new StringBuilder();
		tokenBuilder.append(userName).append(":").append(passwd);
		
		return simpleAuthentication.auth(tokenBuilder.toString()) ? 0 : ReturnCode.USER_FORBID.getCode();
	}

}
