package com.bonree.brfs.common.write.data;

import com.bonree.brfs.common.data.utils.GZipUtils;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年2月4日 下午9:28:15
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 文件解码
 * ****************************************************************************
 */
public class FileDecoder {

    /**
     * 概述：获取版本信息
     *
     * @param bytes
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int version(byte[] bytes) {
        int header = bytes[0] & 0xFF;
        return header >> 5;
    }

    /**
     * 概述：获取大文件检验码类型
     *
     * @param bytes
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int validate(byte[] bytes) {
        int header = bytes[0] & 0xFF;
        return (header >> 3) & 0x03;
    }

    /**
     * 概述：获取内容信息
     *
     * @param bytes 源数据
     * @return
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static FileContent contents(byte[] bytes) throws Exception {
        FileContent.Builder file = FileContent.newBuilder();

        // 获取一条消息
        int dataLength = (int) FSCode.moreFlagDecoder(bytes, 7, 0); // 一条数据的长度
        int dataMoreFlagLength =
            FSCode.moreFlagLength(dataLength, 7) + 1;  // 扩展次数加上moreFlag所在的一个字节.
        byte[] dataBytes = FSCode.subBytes(bytes, dataMoreFlagLength, dataLength);

        // 1.获取压缩标识
        int compressFlag = (dataBytes[0] & 0xFF) >> 6;

        // 描述信息的长度
        int describeLength = (int) FSCode.moreFlagDecoder(dataBytes, 4);
        // 扩展次数加上moreFlag所在的一个字节.
        int describeMoreFlagLength =
            FSCode.moreFlagLength(describeLength, 4) + 1;
        byte[] destResult = FSCode.subBytes(dataBytes, describeMoreFlagLength, describeLength);

        // 内容的开始位置(包含moreflag)
        int contestStart = describeLength + describeMoreFlagLength;
        // 内容的长度
        int contentLength = (int) FSCode.moreFlagDecoder(dataBytes, 7, contestStart);
        // 扩展次数加上moreFlag所在的一个字节.
        int contentMoreFlagLength =
            FSCode.moreFlagLength(contentLength, 7) + 1;
        // 内容的开始位置
        contestStart += contentMoreFlagLength;
        byte[] data = FSCode.subBytes(dataBytes, contestStart, contentLength);

        if (compressFlag == 1) {
            // gzip解压
            destResult = GZipUtils.decompress(destResult);
            data = GZipUtils.decompress(data);
        } else if (compressFlag == 2) {
            // snappy解压
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

    /**
     * 概述：获取大文件校验码
     *
     * @param bytes 源数据字节数组
     * @param pos   检验码的起始位置
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long validate(byte[] bytes, int pos) {
        return FSCode.byteToLong(bytes, pos, 8);
    }

    /**
     * 概述：获取文件offset偏移量
     *
     * @param index 开始位置
     * @param bytes 文件内容
     * @return
     */
    public static int getOffsets(int index, byte[] bytes) {
        int size = 0;
        try {
            int dataLength = (int) FSCode.moreFlagDecoder(bytes, 7, index); // 一条数据的长度
            if (dataLength == 0) {
                return 0;
            }
            int dataMoreFlagLength =
                FSCode.moreFlagLength(dataLength, 7) + 1;  // moreFlag扩展的次数,加上moreFlag所在的一个字节.
            byte[] dataBytes = FSCode.subBytes(bytes, dataMoreFlagLength + index, dataLength);

            int describeLength = (int) FSCode.moreFlagDecoder(dataBytes, 4); // 描述信息的长度
            int describeMoreFlagLength =
                FSCode.moreFlagLength(describeLength, 4) + 1; // moreFlag扩展的次数,加上moreFlag所在的一个字节.

            int totalLength = describeLength + describeMoreFlagLength; // 内容的开始位置(包含moreflag)
            int contentLength = (int) FSCode.moreFlagDecoder(dataBytes, 7, totalLength); // 内容的长度
            int contentMoreFlagLength =
                FSCode.moreFlagLength(contentLength, 7) + 1;  // moreFlag扩展的次数,加上moreFlag所在的一个字节.
            totalLength += contentMoreFlagLength;      // 内容的开始位置
            totalLength += contentLength;

            // 校验码标识
            int crcFlag = (dataBytes[0] & 0x3F) >> 5;
            if (crcFlag == 1) {
                long crcCode = FSCode.moreFlagDecoder(dataBytes, 7, totalLength);
                int crcMoreFlagLength = FSCode.moreFlagLength(crcCode, 7) + 1;
                totalLength += crcMoreFlagLength;
            }
            totalLength += dataMoreFlagLength;
            size = dataLength + dataMoreFlagLength;

            if (totalLength != size) {
                return 0;
            }
        } catch (Exception ex) {
            // ignore
        }
        return size;
    }

    /**
     * 概述：获取文件offset列表
     *
     * @param bytes 文件内容
     * @return
     */
    public static List<String> getDataFileOffsets(byte[] bytes) {
        return getDataFileOffsets(2, bytes);
    }

    public static List<String> getDataFileOffsets(int startOffset, byte[] bytes) {
        List<String> offsetList = new ArrayList<>();
        int begin = startOffset;
        while (begin < bytes.length) {
            try {
                int size = getOffsets(begin, bytes);
                if (size == 0) {
                    break;
                }
                offsetList.add(begin + "|" + size);
                begin += size;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return offsetList;
    }
}
