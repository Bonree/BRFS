package com.bonree.brfs.common.http.netty;

public interface HttpAuthenticator {
	int check(String userName, String passwd);
}
