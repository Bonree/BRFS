package com.bonree.brfs.duplication.catalog;

import com.bonree.brfs.common.proto.InodeProtos.InodeValueProto;
import com.google.protobuf.InvalidProtocolBufferException;

public class InodeValue {
    private InodeValueProto.Builder builder;
    private InodeValueProto proto;

    public InodeValue() {
        builder = InodeValueProto.newBuilder();
    }

    static boolean isFile(byte[] value) throws InvalidProtocolBufferException {
        InodeValueProto inodeValueProto = InodeValueProto.parseFrom(value);
        return inodeValueProto.getFid() != null;
    }

    static String getFid(byte[] value) throws InvalidProtocolBufferException {
        return InodeValueProto.parseFrom(value).getFid();
    }

    public byte[] getInodeID() {
        return proto.getInodeID().getBytes();
    }

    public InodeValueProto.Builder setID(String id) {
        return builder.setInodeID(id);
    }

    public static InodeValue deSerialize(byte[] value) throws InvalidProtocolBufferException {
        InodeValue inodeValue = new InodeValue();
        inodeValue.setProto(InodeValueProto.parseFrom(value));
        return inodeValue;
    }

    private void setProto(InodeValueProto proto) {
        this.proto = proto;
    }

    public String getFid() {
        return proto.getFid();
    }

    public InodeValueProto.Builder setFid(String fid) {
        return builder.setFid(fid);
    }
}
