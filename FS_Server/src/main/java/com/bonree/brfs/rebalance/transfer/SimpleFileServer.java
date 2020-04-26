package com.bonree.brfs.rebalance.transfer;

import com.bonree.brfs.common.utils.PooledThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleFileServer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleFileServer.class);

    private ServerSocket serverSocket;
    private String dataDir;
    private ExecutorService es = null;

    public SimpleFileServer(int port, String dataDir, int threadCount) throws IOException {
        serverSocket = new ServerSocket(port);
        this.dataDir = dataDir;
        es = Executors.newFixedThreadPool(threadCount, new PooledThreadFactory("file_transfer"));
    }

    public void start() {
        LOG.info("start server!!!!");
        while (true) {
            Socket sock = null;
            try {
                sock = serverSocket.accept();
                es.execute(new FileServThread(sock, dataDir, LOG)); // 当成功连接客户端后开启新线程接收文件
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (serverSocket != null) {
            serverSocket.close();
        }
    }
}
