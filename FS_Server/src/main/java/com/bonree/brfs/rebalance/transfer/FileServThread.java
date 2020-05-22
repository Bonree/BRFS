package com.bonree.brfs.rebalance.transfer;

import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.identification.LocalPartitionInterface;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

class FileServThread implements Runnable {

    private Socket sock;
    private LocalPartitionInterface partitionInterface;
    private Logger log;

    FileServThread(Socket sock, LocalPartitionInterface partitionInterface, Logger log) {
        this.sock = sock;
        this.partitionInterface = partitionInterface;
        this.log = log;
    }

    public void run() {
        String ip = sock.getInetAddress().getHostAddress();   // 获取客户端ip
        try {
            log.info("开启新线程接收来自客户端IP: " + ip + " 的文件");
            // 定义socket输入流,接收客户端的信息
            InputStream sockIn = sock.getInputStream();
            // 创建同名文件
            File file = getClientFileName(sockIn);
            if (file == null) {
                writeOutInfo(sock, "存在同名文件或获取文件失败,服务端断开连接!");
                sock.close();
                return;
            }

            FileOutputStream fos = new FileOutputStream(file); // 用来写入硬盘
            byte[] bufFile = new byte[1024 * 1024];   // 接收数据的缓存
            int len = 0;
            while (true) {
                len = sockIn.read(bufFile); // 接收数据
                if (len != -1) {
                    fos.write(bufFile, 0, len); // 写入硬盘文件
                } else {
                    break;
                }
            }
            writeOutInfo(sock, "send file " + file + " finish");   // 文件接收成功后给客户端反馈一个信息
            log.info("文件传送成功!" + System.getProperty("line.separator"));  // 服务端打印一下
            fos.close();
            sock.close();
        } catch (Exception ex) {
            throw new RuntimeException(ip + "异常!!!", ex);
        }
    }

    public void writeOutInfo(Socket sock, String infoStr) throws Exception {    // 将信息反馈给服务端
        OutputStream sockOut = sock.getOutputStream();
        sockOut.write(infoStr.getBytes(StandardCharsets.UTF_8));
    }

    public File getClientFileName(InputStream sockIn) throws Exception {    // 获取文件名并创建
        // 获取客户端请求发送的文件名,创建路径
        byte[] bufName = new byte[1024];
        int lenInfo = 0;
        lenInfo = sockIn.read(bufName);  // 获取文件名
        String transferFileName = new String(bufName, 0, lenInfo, StandardCharsets.UTF_8);
        log.info("transferFileName: {}", transferFileName);

        String[] split = StringUtils.split(transferFileName, ":");
        String partitionId = StringUtils.substringAfterLast(split[0], "/");
        String datePath = StringUtils.substringBeforeLast(split[0], "/");
        String fileName = split[1];

        String dataDir = this.partitionInterface.getDataPaths(partitionId);
        if (dataDir == null) {
            log.warn("get partition path is null");
            return null;
        }

        log.info("get partition path by partition id, partition id:{}, dataDir:{}", partitionId, dataDir);
        String filePath = dataDir + FileUtils.FILE_SEPARATOR + datePath + FileUtils.FILE_SEPARATOR + fileName;

        File file = new File(filePath);  //保存到相应的位置
        if (file.isDirectory()) {
            log.info(file.getName() + "不能传输目录,断开该ip连接." + System.getProperty("line.separator"));
            writeOutInfo(sock, "服务端不能传输目录!"); // 反馈给客户端的信息
            return null;
        }
        if (file.exists()) {
            log.info(file.getName() + "文件已存在,断开该ip连接." + System.getProperty("line.separator"));
            writeOutInfo(sock, "服务端已存在同名文件!"); // 反馈给客户端的信息
            return null;
        }
        log.info("将客户端发来的文件 {} 存到 {}", fileName, file.getAbsolutePath());
        FileUtils.createFile(filePath, true);
        log.info("成功创建文件 {} 准备写入数据", fileName);
        writeOutInfo(sock, "FileSendNow");    // 告诉客户端,开始传送数据吧
        return file;

    }
}