package com.bonree.brfs.duplication.datastream.connection.tcp;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.net.tcp.client.TcpClientCloseListener;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.TcpDiskNodeClient;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import java.io.IOException;

public class TcpDiskNodeConnection implements DiskNodeConnection {
    private TcpClient<BaseMessage, BaseResponse> tcpClient;
    private TcpDiskNodeClient nodeClient;

    private volatile boolean connected;

    public TcpDiskNodeConnection(TcpClient<BaseMessage, BaseResponse> tcpClient) {
        this.tcpClient = tcpClient;
        this.nodeClient = new TcpDiskNodeClient(tcpClient);

        this.connected = true;
        this.tcpClient.setClientCloseListener(new TcpClientCloseListener() {

            @Override
            public void clientClosed() {
                connected = false;
            }
        });
    }

    @Override
    public void close() throws IOException {
        connected = false;
        tcpClient.close();
    }

    @Override
    public String getRemoteAddress() {
        return tcpClient.remoteHost();
    }

    @Override
    public int getRemotePort() {
        return tcpClient.remotePort();
    }

    @Override
    public boolean isValid() {
        return connected;
    }

    @Override
    public DiskNodeClient getClient() {
        return nodeClient;
    }

}
