package com.bonree.brfs.server.utils;

import com.bonree.brfs.common.code.FSCode;
import com.bonree.brfs.common.proto.FileDataProtos.FileContent;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年2月4日 下午9:28:15
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 文件解码
 *****************************************************************************
 */
public class FileDecoder {

    /**
     * 概述：获取版本信息
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
     * @param bytes 源数据
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static FileContent contents(byte[] bytes) {
        FileContent.Builder file = FileContent.newBuilder();
        int describeLength = (int) FSCode.moreFlagDecoder(bytes, 4);// 描述信息的长度
        int describeMoreFlagLength = FSCode.moreFlagLength(describeLength, 4) + 1;// 扩展次数加上moreFlag所在的一个字节.
        byte[] destResult = FSCode.subBytes(bytes, describeMoreFlagLength, describeLength);

        int contestStart = describeLength + describeMoreFlagLength; // 内容的开始位置(包含moreflag)
        int contentLength = (int) FSCode.moreFlagDecoder(bytes, 7, contestStart); // 内容的长度
        int contentMoreFlagLength = FSCode.moreFlagLength(contentLength, 7) + 1;  // 扩展次数加上moreFlag所在的一个字节.
        contestStart += contentMoreFlagLength;      // 内容的开始位置
        byte[] data = FSCode.subBytes(bytes, contestStart, contentLength);

        int compress = (bytes[0] & 0xFF) >> 6;// 获取压缩标识
        if (compress == 1) {        // gzip解压
            // result =
            // data =
        } else if (compress == 2) { // snappy解压
            // result =
            // data =
        }
        file.setCompress(compress);
        if (destResult != null && destResult.length != 0) {
            file.setDescription(new String(destResult));
        }
        if (data != null && data.length != 0) {
            file.setData(new String(data));
        }

        int crcFlag = (bytes[0] & 0xFF) >> 5;// 获取校验码标识
        if (crcFlag == 1) {
            int crcStart = contestStart + contentLength;
            long crcCode = FSCode.moreFlagDecoder(bytes, 7, crcStart);
            file.setCrcCheckCode(crcCode);
            file.setCrcFlag(true);
        }

        return file.build();
    }

    /**
     * 概述：获取大文件校验码
     * @param bytes 源数据字节数组
     * @param pos 检验码的起始位置
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long validate(byte[] bytes, int pos) {
        return FSCode.byteToLong(bytes, pos, 8);
    }

}
