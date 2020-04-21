package com.bonree.brfs.common.net.http.data;

import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author wangchao
 */
public class FSPacket {
    private static final int MAX_DATA_LEN = 64 * 1024;
    private FSPacketProto proto;
    private FSPacketProto.Builder builder;

    public FSPacket(){} //use for deserialize
    public FSPacket(int storageName,String fileName, long offsetInFile, long seqno,
                        byte[] data) {

        Preconditions.checkArgument(data.length <= MAX_DATA_LEN,
                "the data is too large to a packet. " +
                        "the max data length should less than [{}] byte,now it is [{}]byte"
                ,MAX_DATA_LEN
                ,data.length);
        Preconditions.checkArgument(null != fileName && !"".equals(fileName),
                "[{}]is not a valid fileName!!you must specify the fileName",fileName);
        builder = FSPacketProto.newBuilder();
        builder.setStorageName(storageName)
                .setCrcFlag(false)
                .setFileName(fileName)
                .setOffsetInFile(offsetInFile)
                .setSeqno(seqno)
                .setData(ByteString.copyFrom(data))
                .setLastPacketInFile(false);
    }

    /**
     * 填充校验和
     * @param checkSum 校验和
     */
    public void setCheckSum(int checkSum){
        builder.setCrcFlag(true);
        builder.setCrcCheckCode(checkSum);
    }

    /**
     * 设置为该文件的最后一个包，发送最后一个包的时候调用
     */
    public void setLastPacketInFile(){
        builder.setLastPacketInFile(true);
    }
    // setters and getters
    public boolean isLastPacketInFile(){
        return proto.getLastPacketInFile();
    }
    public int getStorageName(){
        return proto.getStorageName();
    }
    public String getFileName(){
        return proto.getFileName();
    }
    public long getOffsetInFile(){
        return proto.getOffsetInFile();
    }
    public byte[] getData(){
        return proto.getData().toByteArray();
    }
    public long getSeqno(){return proto.getSeqno();}
    /**
     * 构建数据包对象,序列化之前调用，来生成真正的proto
     */
    public FSPacket build(){
        proto = builder.build();
        return this;
    }

    public byte[] serialize(){
        Preconditions.checkNotNull(proto,"trying use the DFPacket before build it,you can build it use the method this.build()");
        return proto.toByteArray();
    }

    public FSPacket deserialize(byte[] data) throws InvalidProtocolBufferException {
        proto = FSPacketProto.parseFrom(data);
        return this;
    }
    //连续的一堆packet在同一个block中,获得该packet属于哪个block
    public long getBlockOffsetInFile(long blockSize){
        return (getOffsetInFile()/blockSize)*blockSize;
    }
    //是不是一个block的第一个packet
    public boolean isTheFirstPacketInFile(){
        return getOffsetInFile()==0;
    }
    //释放protobuf反序列化的对象
    public void clearData(){
        proto = null;
    }

    @Override
    public String toString() {
        return "FSPacket of StorageName:["+getStorageName()+
                "] file["+getFileName()+
                "] offset:["+getOffsetInFile()+"] len:["+getData().length+
                "]byte seqno: ["+getSeqno()+
                "] islast[" +isLastPacketInFile()+"]";
    }

    /**
     * 获得packet在block中的position
     * @param blockSize
     * @return
     */
    public int getPacPos(long blockSize) {
        //返回packet的pos  这个值应该小于1024，因为一个block最多存1024个packet
        return (int)(getOffsetInFile()-getBlockOffsetInFile(blockSize));

    }

    public boolean isATinyFile() {
        return isLastPacketInFile()&&isTheFirstPacketInFile();
    }
    public void setProto(FSPacketProto proto){
        this.proto = proto;
    }

    public String getWriteID() {
        return proto.getWriteID();
    }
}
