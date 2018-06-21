package com.bonree.brfs.common.net.http.netty;

public interface HttpAuthenticator {
	int check(String userName, String passwd);
}
