package com.bonree.brfs.common.net.tcp;

public interface MessageHandler<T> {
    void handleMessage(BaseMessage baseMessage, ResponseWriter<T> writer);
}
