package com.bonree.brfs.server.utils;

import com.bonree.brfs.common.code.FSCode;

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
     * 概述：获取检验标识
     * @param bytes
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int validate(byte[] bytes) {
        int header = bytes[0] & 0xFF;
        return (header >> 3) & 0x03;
    }

    /**
     * 概述：获取描述信息
     * @param bytes 源数据
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String description(byte[] bytes) {
        int describeLength = (int) FSCode.moreFlagDecoder(bytes, 5);        // 描述信息的长度
        int moreFlagLength = FSCode.moreFlagLength(describeLength, 5) + 1;  // 扩展次数加上moreFlag所在的一个字节.
        byte[] result = FSCode.subBytes(bytes, moreFlagLength, describeLength);
        int compress = bytes[0] >> 6;// 获取压缩标识
        if (compress == 1) {        // gzip解压
            // result =
        } else if (compress == 2) { // snappy解压
            // result =
        }
        return new String(result);
    }

    /**
     * 概述：获取内容信息
     * @param bytes 源数据
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String content(byte[] bytes) {
        int describeLength = (int) FSCode.moreFlagDecoder(bytes, 5);// 描述信息的长度
        int describeMoreFlagLength = FSCode.moreFlagLength(describeLength, 5) + 1;// 扩展次数加上moreFlag所在的一个字节.
        int contestStart = describeLength + describeMoreFlagLength; // 内容的开始位置(包含moreflag)
        int contentLength = (int) FSCode.moreFlagDecoder(bytes, 7, contestStart); // 内容的长度
        int contentMoreFlagLength = FSCode.moreFlagLength(contentLength, 7) + 1;  // 扩展次数加上moreFlag所在的一个字节.
        contestStart += contentMoreFlagLength;      // 内容的开始位置
        byte[] result = FSCode.subBytes(bytes, contestStart, contentLength);
        int compress = (bytes[0] & 0xFF) >> 6;// 获取压缩标识
        if (compress == 1) {        // gzip解压
            // result =
        } else if (compress == 2) { // snappy解压
            // result =
        }
        return new String(result);
    }

    /**
     * 概述：获取校验码
     * @param bytes 源数据字节数组
     * @param pos 检验码的起始位置
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String validate(byte[] bytes, int pos) {
        int validateLength = (int) FSCode.moreFlagDecoder(bytes, 7);        // 检验码的长度
        int moreFlagLength = FSCode.moreFlagLength(validateLength, 5) + 1;  // 扩展次数加上moreFlag所在的一个字节.
        pos += moreFlagLength;
        byte[] result = FSCode.subBytes(bytes, pos, validateLength);
        return new String(result);
    }

}
