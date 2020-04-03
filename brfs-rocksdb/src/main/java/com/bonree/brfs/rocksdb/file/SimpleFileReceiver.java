package com.bonree.brfs.rocksdb.file;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年12月19日 下午2:43:00
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 文件接收助手，使用原生TCP实现
 ******************************************************************************/
public class SimpleFileReceiver implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFileReceiver.class);

    private String ip;
    private int transPort;
    private ServerSocket serverSocket;
    private ExecutorService bossThread;
    private ExecutorService workThreads;
    private String transferFile;
    private boolean isRunning;

    public SimpleFileReceiver(int transPort, int transThreads, String transferFile) {
        this("0.0.0.0", transPort, transThreads, transferFile);
    }

    public SimpleFileReceiver(String ip, int transPort, int transThreads, String transferFile) {

        this.ip = ip;
        this.transPort = transPort;
        this.transferFile = transferFile;

        bossThread = Executors.newSingleThreadExecutor(new PooledThreadFactory("file-server"));
        workThreads = Executors.newFixedThreadPool(transThreads, new PooledThreadFactory("file-thread"));
        isRunning = true;
    }

    @Override
    public void start() throws Exception {
        InetAddress addr = InetAddress.getByName(ip);
        serverSocket = new ServerSocket(transPort, 50, addr);
        LOG.info("start fileReceiver...");
        bossThread.execute(new ListenerRunner());
    }

    @Override
    public void stop() throws Exception {
        if (bossThread != null) {
            bossThread.shutdown();
        }
        if (serverSocket != null) {
            serverSocket.close();
        }
        if (workThreads != null) {
            workThreads.shutdown();
        }
    }

    private void receiveFile(Socket socket) throws Exception {
        if (socket == null) {
            throw new Exception("illegal socket");
        }
        String ip = socket.getInetAddress().getHostAddress();
        FileOutputStream fos = null;
        try {
            LOG.info("receive file from {}", ip);
            File file = new File(transferFile);
            Files.createParentDirs(file);

            InputStream sockIn = socket.getInputStream();
            fos = new FileOutputStream(file);
            byte[] bufFile = new byte[1024 * 1024];
            int len;
            while (-1 != (len = sockIn.read(bufFile))) {
                fos.write(bufFile, 0, len);
            }
            LOG.info("{} finish to receive", file.getAbsolutePath());
        } finally {
            if (fos != null) {
                fos.close();
            }
            socket.close();
        }
    }

    class ListenerRunner implements Runnable {
        @Override
        public void run() {
            try {
                while (isRunning) {
                    if (serverSocket.isClosed()) {
                        isRunning = false;
                        break;
                    }

                    final Socket clientSocket = serverSocket.accept();
                    workThreads.execute(() -> {
                        try {
                            receiveFile(clientSocket);
                        } catch (Exception e) {
                            LOG.error("receive file error:", e);
                        }
                    });
                }
            } catch (Exception e) {
                LOG.error("file receiver error:", e);
            }
        }

    }

}
