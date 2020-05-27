package com.bonree.brfs.rebalance.transfer;

import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.identification.LocalPartitionInterface;
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
    private LocalPartitionInterface partitionInterface;
    private ExecutorService es = null;
    private boolean run;

    public SimpleFileServer(int port, LocalPartitionInterface partitionInterface, int threadCount) throws IOException {
        serverSocket = new ServerSocket(port);
        this.partitionInterface = partitionInterface;
        es = Executors.newFixedThreadPool(threadCount, new PooledThreadFactory("file_transfer"));
        run = true;
    }

    public void start() {
        LOG.info("start file server!!!");
        while (run) {
            Socket sock = null;
            try {
                sock = serverSocket.accept();
                es.execute(new FileServThread(sock, partitionInterface, LOG)); // 当成功连接客户端后开启新线程接收文件
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws IOException {
        run = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
    }
}
