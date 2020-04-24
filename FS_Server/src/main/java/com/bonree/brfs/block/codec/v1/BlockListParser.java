package com.bonree.brfs.block.codec.v1;

import com.bonree.brfs.common.data.utils.GZipUtils;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;
import com.bonree.brfs.common.write.data.FSCode;
import com.google.protobuf.ByteString;

public class BlockListParser {
	
	public FileContent parse(byte[] bytes) throws Exception {
        FileContent.Builder file = FileContent.newBuilder();

        // 获取一条消息
        int dataLength = (int) FSCode.moreFlagDecoder(bytes, 7, 0); // 一条数据的长度
        int dataMoreFlagLength = FSCode.moreFlagLength(dataLength, 7) + 1;  // 扩展次数加上moreFlag所在的一个字节.
        byte[] dataBytes = FSCode.subBytes(bytes, dataMoreFlagLength, dataLength);

        // 1.获取压缩标识
        int compressFlag = (dataBytes[0] & 0xFF) >> 6;

        int describeLength = (int) FSCode.moreFlagDecoder(dataBytes, 4);// 描述信息的长度
        int describeMoreFlagLength = FSCode.moreFlagLength(describeLength, 4) + 1;// 扩展次数加上moreFlag所在的一个字节.
        byte[] destResult = FSCode.subBytes(dataBytes, describeMoreFlagLength, describeLength);

        int contestStart = describeLength + describeMoreFlagLength; // 内容的开始位置(包含moreflag)
        int contentLength = (int) FSCode.moreFlagDecoder(dataBytes, 7, contestStart); // 内容的长度
        int contentMoreFlagLength = FSCode.moreFlagLength(contentLength, 7) + 1;  // 扩展次数加上moreFlag所在的一个字节.
        contestStart += contentMoreFlagLength;      // 内容的开始位置
        byte[] data = FSCode.subBytes(dataBytes, contestStart, contentLength);

        if (compressFlag == 1) {        // gzip解压
            destResult = GZipUtils.decompress(destResult);
            data = GZipUtils.decompress(data);
        } else if (compressFlag == 2) { // snappy解压
            // destResult =
            // data =
        }
        file.setCompress(compressFlag);

        // 2.封装描述信息
        if (destResult != null && destResult.length != 0) {
            file.setDescription(new String(destResult));
        }

        // 3.封装数据内容
        if (data != null && data.length != 0) {
            file.setData(ByteString.copyFrom(data));
        }

        // 4.校验码标识
        int crcFlag = (dataBytes[0] & 0x3F) >> 5;
        if (crcFlag == 1) {
            int crcStart = contestStart + contentLength;
            long crcCode = FSCode.moreFlagDecoder(dataBytes, 7, crcStart);
            file.setCrcCheckCode(crcCode);
            file.setCrcFlag(true);
        }
        return file.build();
    }
}
