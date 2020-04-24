package com.bonree.brfs.rebalance.transfer;

import com.bonree.brfs.common.utils.FileUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleFileClient {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFileClient.class);

    public void sendFile(String ip, int port, String localFilePath, String remoteDir, String fileName) throws Exception {
        String remotePath = remoteDir + FileUtils.FILE_SEPARATOR + fileName;
        Socket sock = null;
        FileInputStream fis = null;
        try {
            LOG.info("send file:" + remotePath);
            File file = new File(localFilePath);
            if (file.isFile()) {
                sock = new Socket(ip, port); // 指定服务端地址和端口
                fis = new FileInputStream(file); // 读取本地文件
                OutputStream sockOut = sock.getOutputStream();   // 定义socket输出流
                LOG.info("待发送文件:" + remotePath);
                sockOut.write(remotePath.getBytes(StandardCharsets.UTF_8));
                String serverInfo = servInfoBack(sock); // 反馈的信息:服务端是否获取文件名并创建文件成功
                if (serverInfo.equals("FileSendNow")) {
                    // 服务端说已经准备接收文件,发吧
                    byte[] bufFile = new byte[1024];
                    int len = 0;
                    while (true) {
                        len = fis.read(bufFile);
                        if (len != -1) {
                            sockOut.write(bufFile, 0, len); // 将从硬盘上读取的字节数据写入socket输出流
                        } else {
                            break;
                        }
                    }
                } else {
                    LOG.info("服务端返回信息:" + serverInfo);
                }
                sock.shutdownOutput();   // 必须的,要告诉服务端该文件的数据已写完
                LOG.info("服务端最后一个返回信息:" + servInfoBack(sock)); // 显示服务端最后返回的信息
            } else {
                LOG.info("要发送的文件 " + localFilePath + " 不是一个标准文件,请正确指定");
            }
        } finally {
            if (fis != null) {
                fis.close();
            }
            if (sock != null) {
                sock.close();
            }
        }
    }

    public String servInfoBack(Socket sock) throws Exception {
        // 读取服务端的反馈信息
        InputStream sockIn = sock.getInputStream(); // 定义socket输入流
        byte[] bufIn = new byte[1024];
        int lenIn = sockIn.read(bufIn);            // 将服务端返回的信息写入bufIn字节缓冲区
        String info = "";
        if (lenIn > 0) {
            info = new String(bufIn, 0, lenIn, StandardCharsets.UTF_8);
        }
        return info;
    }

    public static void main(String[] args) throws Exception {
        //        for (int i = 0; i < 1000; i++) {
        //            FileUtils.writeFileFromList("E:/111/"+i, Lists.newArrayList("aaaa"));
        //        }
        //        SimpleFileClient client = new SimpleFileClient();
        //        File dir = new File("E:/111");
        //        File[] files = dir.listFiles();
        //        for (int i = 0; i < files.length; i++) {
        //            client.sendFile("192.168.4.111", 10007, files[i].getAbsolutePath(), "111", files[i].getName());
        //        }

        // client.sendFile("192.168.4.111", 10007, "d:/apache-maven-3.2.5.rar", "111", "apache-maven-3.2.5.rar");

    }
}
