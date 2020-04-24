package com.bonree.brfs.common.net.http.data;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author wangchao
 */
public class FSPacketUtil {
    public static byte[] serialize(FSPacket p) {
        return p.serialize();
    }

    public static FSPacket deserialize(byte[] data) throws InvalidProtocolBufferException {
        return new FSPacket().deserialize(data);
    }
}
