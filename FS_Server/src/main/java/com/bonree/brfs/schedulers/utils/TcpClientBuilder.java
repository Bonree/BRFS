package com.bonree.brfs.schedulers.utils;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.client.TaskTcpClientGroup;
import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.net.tcp.client.TcpClientConfig;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.net.tcp.file.client.AsyncFileReaderCreateConfig;
import com.bonree.brfs.common.net.tcp.file.client.AsyncFileReaderGroup;
import com.bonree.brfs.common.net.tcp.file.client.FileContentPart;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.disknode.client.TcpDiskNodeClient;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

public class TcpClientBuilder {
    private static final int DEFAULT_IDLE_TIME_SECOND = 60;
    private static final int DEFAULT_READ_IDLE_TIME_SECOND = 30;
    private static final int DEFAULT_WRITE_IDLE_TIME_SECOND = 30;
    private static final int DEFAULT_TIME_OUT = 10000;
    private int idleTime;
    private int readIdleTime;
    private int writeIdleTime;
    private int timeout;
    private TaskTcpClientGroup group;
    private AsyncFileReaderGroup group2;

    public TcpClientBuilder() {
        this(DEFAULT_IDLE_TIME_SECOND, DEFAULT_READ_IDLE_TIME_SECOND, DEFAULT_WRITE_IDLE_TIME_SECOND, DEFAULT_TIME_OUT);
    }

    public TcpClientBuilder(int idleTime, int readIdleTime, int writeIdleTime, int timeout) {
        this.idleTime = idleTime;
        this.readIdleTime = readIdleTime;
        this.writeIdleTime = writeIdleTime;
        this.timeout = timeout;
        group = new TaskTcpClientGroup(4, this.idleTime, this.readIdleTime, this.writeIdleTime);
        group2 = new AsyncFileReaderGroup(4);
    }

    public TcpDiskNodeClient getClient(Service service, int timeout) throws InterruptedException {
        TcpClient<BaseMessage, BaseResponse> tcpClient = group.createClient(new TcpClientConfig() {
            @Override
            public SocketAddress remoteAddress() {
                return new InetSocketAddress(service.getHost(), service.getPort());
            }

            @Override
            public int connectTimeoutMillis() {
                return timeout;
            }
        });

        TcpClient<ReadObject, FileContentPart> readerClient = group2.createClient(new AsyncFileReaderCreateConfig() {
            @Override
            public SocketAddress remoteAddress() {
                return new InetSocketAddress(service.getHost(), service.getExtraPort());
            }

            @Override
            public int connectTimeoutMillis() {
                return timeout;
            }

            @Override
            public int maxPendingRead() {
                return 0;
            }
        });
        return new TcpDiskNodeClient(tcpClient, readerClient);
    }

    public TcpDiskNodeClient getClient(Service service) throws InterruptedException {
        return getClient(service, timeout);
    }

    public static void main(String[] args) throws Exception {
        String host = "192.168.150.237";
        int port = 9881;
        int export = 9900;
        Service service = new Service("10", "data_group", host, port);
        service.setExtraPort(export);

        TcpDiskNodeClient client = new TcpClientBuilder().getClient(service);
        List<com.bonree.brfs.disknode.server.handler.data.FileInfo> list =
            client.listFiles("/delSr1/1/2020/05/07/21_00_00/0_22", 1);
        list.stream().forEach(x -> {
            System.out.println(x.getPath());
        });
    }
}
