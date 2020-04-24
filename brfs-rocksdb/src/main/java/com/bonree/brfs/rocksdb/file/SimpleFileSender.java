package com.bonree.brfs.rocksdb.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年12月13日 上午10:45:00
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 文件发送助手
 ******************************************************************************/
public class SimpleFileSender {

    private final static Logger LOG = LoggerFactory.getLogger(SimpleFileSender.class);

    /**
     * 接收端ip，端口，待发送文件路径，接收端文件路径
     */
    public void send(String recvrAddr, int port, String filePathName, String remoteDir) throws Exception {
        File file = new File(filePathName);
        if (!file.exists()) {
            LOG.error("file:{} is not exist!", filePathName);
            throw new Exception("file is not exist!");
        }
        send(recvrAddr, port, file, remoteDir);
    }

    /**
     * 接收端ip，端口，待发送文件实例，接收端文件路径
     */
    public void send(String recvrAddr, int port, File file, String remoteDir) throws Exception {
        if (file.isDirectory()) {
            sendDir(recvrAddr, port, file.getName(), file, remoteDir);
        } else if (file.isFile()) {
            sendFile(recvrAddr, port, file.getName(), file, remoteDir);
        } else {
            LOG.error("unknown file type!");
            throw new Exception("unknown file type!");
        }
    }

    /**
     * 概述：发送目录，实际上是发送目录里的文件
     *
     * @param recvrAddr
     * @param port
     * @param relativePath
     * @param file
     *
     * @throws Exception
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private void sendDir(String recvrAddr, int port, String relativePath, File file, String remoteDir) throws Exception {
        LOG.info("send dir:{}", relativePath);
        File[] files = file.listFiles();
        if (files != null) {
            // 解决空目录无法deploy问题
            if (files.length == 0) {
                sendFile(recvrAddr, port, relativePath, null, remoteDir);
            }
            for (File subFile : files) {
                if (subFile.isDirectory()) {
                    sendDir(recvrAddr, port, relativePath + FileTransUtil.FILE_SEPARATOR + subFile
                        .getName(), subFile, remoteDir);
                } else {
                    sendFile(recvrAddr, port, relativePath + FileTransUtil.FILE_SEPARATOR + subFile
                        .getName(), subFile, remoteDir);
                }
            }
        }
    }

    /**
     * 概述：发送文件，真正干活的方法
     *
     * @param recvrAddr
     * @param port
     * @param relativePath
     * @param file
     *
     * @throws Exception
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private void sendFile(String recvrAddr, int port, String relativePath, File file, String remoteDir) throws Exception {
        Socket sock = null;
        FileInputStream fis = null;
        try {
            sock = new Socket(recvrAddr, port);
            OutputStream sockOut = sock.getOutputStream();

            fis = new FileInputStream(file);
            LOG.info("prepare send file [{}] to [{}], remoteDir: [{}]", relativePath, recvrAddr, remoteDir);

            byte[] bufFile = new byte[1024];
            int len;
            while (-1 != (len = fis.read(bufFile))) {
                sockOut.write(bufFile, 0, len);
            }
            sock.shutdownOutput();
            LOG.info("send file [{}] to [{}] complete", file.getAbsolutePath(), recvrAddr);
        } finally {
            if (fis != null) {
                fis.close();
            }
            if (sock != null) {
                sock.close();
            }
        }
    }

}
