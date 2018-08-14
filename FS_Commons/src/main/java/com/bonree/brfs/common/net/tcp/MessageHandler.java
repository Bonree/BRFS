package com.bonree.brfs.common.net.tcp;


public interface MessageHandler {
	void handleMessage(BaseMessage baseMessage, HandleCallback callback);
}
