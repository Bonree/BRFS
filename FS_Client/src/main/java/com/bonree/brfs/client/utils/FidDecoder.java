package com.bonree.brfs.client.utils;

import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.common.code.Base64;
import com.bonree.brfs.common.code.FSCode;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年1月29日 下午2:05:46
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: fid解码
 *****************************************************************************
 */
public class FidDecoder {

    /**
     * 概述：fid信息解码
     * @param fidStr base64格式的fid
     * @return
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static Fid build(String fidStr) throws Exception {
        Fid.Builder fid = Fid.newBuilder();
        if (fidStr != null) {
            byte[] bytes = Base64.decode(fidStr, Base64.DEFAULT);
            fid.setVersion(version(bytes));
            fid.setCompress(compress(bytes));
            fid.setStorageNameCode(storageName(bytes));
            fid.setTime(time(bytes));
            fid.setUuid(uuid(bytes));
            fid.setOffset(offset(bytes));
            fid.setSize(size(bytes));
            fid.addAllServerId(serverId(bytes));
        }
        return fid.build();
    }

    /**
     * 概述：解码version
     * @param header
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int version(byte[] bytes) {
        int h = bytes[1] & 0xff;
        return h >> 5;
    }

    /**
     * 概述：解码compress
     * @param header
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static int compress(byte[] bytes) {
        int h = bytes[1] & 0xff;
        return h >> 3 & 0x03;
    }

    /**
     * 概述：解码storageName
     * @param bytes
     * @return
     * @throws Exception 
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long storageName(byte[] bytes) throws Exception {
        return FSCode.byteToLong(bytes, 2, 2);
    }

    /**
     * 概述：解码uuid
     * @param bytes
     * @return
     * @throws Exception 
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String uuid(byte[] bytes) throws Exception {
        return FSCode.ByteToHex(bytes, 4, 16);
    }

    /**
     * 概述：解码time
     * @param timeByte
     * @return
     * @throws Exception 
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long time(byte[] bytes) throws Exception {
        return FSCode.byteToLong(bytes, 20, 5) * 60 * 1000;
    }

    /**
     * 概述：解码offset
     * @param offsetByte
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long offset(byte[] offsetByte) {
        return FSCode.byteToLong(offsetByte, 25, 4);
    }

    /**
     * 概述：解码size
     * @param sizeByte
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static long size(byte[] sizeByte) {
        return FSCode.byteToLong(sizeByte, 29, 4);
    }

    /**
     * 概述：解码serverId
     * @param serverIdByte
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static List<Integer> serverId(byte[] bytes) {
        int temp = 0;
        List<Integer> sidList = new ArrayList<Integer>();
        for (int i = 33; i < bytes.length; i++) {
            int sid = bytes[i] & 0xFF;
            if (sid >> 7 == 0) {
                temp <<= 7;
                sidList.add(temp | (sid & 0x7F));
                temp = 0;
            } else {
                temp = sid & 0x7F;
            }
        }
        return sidList;
    }

}
