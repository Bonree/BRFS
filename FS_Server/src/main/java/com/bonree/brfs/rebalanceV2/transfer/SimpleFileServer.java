package com.bonree.brfs.rebalanceV2.transfer;

import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.identification.LocalPartitionInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleFileServer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleFileServer.class);

    private ServerSocket serverSocket;
    private LocalPartitionInterface partitionInterface;
    private ExecutorService es = null;

    public SimpleFileServer(int port, LocalPartitionInterface partitionInterface, int threadCount) throws IOException {
        serverSocket = new ServerSocket(port);
        this.partitionInterface = partitionInterface;
        es = Executors.newFixedThreadPool(threadCount, new PooledThreadFactory("file_transfer"));
    }

    public void start() {
        LOG.info("start server!!!!");
        while (true) {
            Socket sock = null;
            try {
                sock = serverSocket.accept();
                es.execute(new FileServThread(sock, partitionInterface, LOG));// 当成功连接客户端后开启新线程接收文件
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
