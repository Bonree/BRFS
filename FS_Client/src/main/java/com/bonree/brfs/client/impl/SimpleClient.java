package com.bonree.brfs.client.impl;

import com.bonree.brfs.client.utils.InputUtils;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.write.data.FidDecoder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.apache.curator.shaded.com.google.common.base.Joiner;
import org.apache.curator.shaded.com.google.common.primitives.Ints;

public class SimpleClient {
    private byte[] request;
    private Socket socket;

    public SimpleClient(String fid, String ip, int port, String sr, int index) throws Exception {
        Fid fidObj = FidDecoder.build(fid);

        StringBuilder nameBuilder = new StringBuilder(fidObj.getUuid());
        String[] serverList = new String[fidObj.getServerIdCount()];
        for (int i = 0; i < fidObj.getServerIdCount(); i++) {
            String id = fidObj.getServerId(i);
            nameBuilder.append('_').append(id);
            serverList[i] = id;
        }

        ReadObject readObject = new ReadObject();
        readObject.setSn(sr);
        readObject.setIndex(index);
        readObject.setTime(fidObj.getTime());
        readObject.setDuration(fidObj.getDuration());
        readObject.setFileName(nameBuilder.toString());
        readObject.setOffset(fidObj.getOffset());
        readObject.setLength((int) fidObj.getSize());

        readObject.setToken(0);
        request = Joiner.on(';').useForNull("-")
                        .join(readObject.getSn(),
                              readObject.getIndex(),
                              readObject.getTime(),
                              readObject.getDuration(),
                              readObject.getFileName(),
                              readObject.getFilePath(),
                              readObject.getOffset(),
                              readObject.getLength(),
                              readObject.getRaw(),
                              readObject.getToken(),
                              "\n").getBytes(Charsets.UTF_8);

        socket = new Socket();
        socket.connect(new InetSocketAddress(ip, port));
    }

    public byte[] read() throws IOException {
        socket.getOutputStream().write(request);

        byte[] length = new byte[Integer.BYTES * 2];
        InputUtils.readBytes(socket.getInputStream(), length, 0, length.length);

        int l = Ints.fromBytes(length[4], length[5], length[6], length[7]);

        byte[] b = new byte[l];
        InputUtils.readBytes(socket.getInputStream(), b, 0, b.length);

        return b;
    }

    public void close() throws IOException {
        socket.close();
    }
}
