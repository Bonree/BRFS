package com.bonree.brfs.common.http.netty;

public interface HttpAuthenticator {
	boolean isLegal(String userName, String passwd);
}
