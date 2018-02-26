package com.bonree.brfs.server.utils;

import com.bonree.brfs.common.code.Base64;
import com.bonree.brfs.common.code.FSCode;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年1月29日 下午2:06:04
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: fid编码
 *****************************************************************************
 */
public class FidEncoder {

    /**
     * 概述：生成fid
     * @param fid 封装fid的数据对象
     * @return
     * @throws Exception
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    public static String build(Fid fid) throws Exception {
        if (validate(fid)) {
            System.out.println("validate failed!");
            return "";
        }
        byte[] header = header(fid.getVersion(), fid.getCompress(), fid.getReplica());
        byte[] storageName = storageName(fid.getStorageNameCode());
        byte[] uuid = uuid(fid.getUuid());
        byte[] time = time(fid.getTime());
        byte[] offset = offset(fid.getOffset());
        byte[] size = size(fid.getSize());
        byte[] serverId = serverId(fid.getServerId());
        // 封装fid
        byte[] fidByte = FSCode.addBytes(FSCode.start, header, storageName, uuid, time, offset, size, serverId, FSCode.tail);
        return Base64.encodeToString(fidByte, Base64.DEFAULT);
    }
    
    /**
     * 概述：fid相关属性验证
     * @param fid
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static boolean validate(Fid fid) {
        if (fid.getVersion() < 0 || fid.getVersion() > 7) { // version取值范围0~7
            return true;
        }
        if (fid.getCompress() < 0 || fid.getCompress() > 3) { // compress取值范围0~3
            return true;
        }
        if (fid.getStorageNameCode() <= 0 || fid.getStorageNameCode() > 65535) { // storageNameCode取值范围0~65535
            return true;
        }
        if (fid.getUuid() == null || fid.getUuid().length() > 32 || fid.getUuid().length() % 2 != 0) { // uuid长度为32字节
            return true;
        }
        if (fid.getTime() <= 0 || fid.getTime() > 4701945540L) { // time取值范围可到2118-12-31 23:59
            return true;
        }
        if (fid.getServerId() == null) {
            return true;
        } else {
            String[] sidArr = fid.getServerId().split("_");
            for (String sidStr : sidArr) {
                int sid = Integer.valueOf(sidStr);
                if (sid > 16383) { // serverId取值范围是0~16383
                    return true;
                }
            }
        }
        if (fid.getOffset() <= 0 || fid.getOffset() > 4294967295L) { // offset取值范围0~4294967295
            return true;
        }
        if (fid.getSize() <= 0 || fid.getSize() > 4294967295L) { // size取值范围0~4294967295
            return true;
        }
        return false;
    }

    /**
     * 概述： 封装header
     * @param version 版本号
     * @param compress 压缩标识
     * @param replica 副本数
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static byte[] header(int version, int compress, int replica) {
        int v = version << 5;
        int c = compress << 3;
        int r = replica & 0x07;
        return new byte[] { (byte) (v | c | r) };
    }

    /**
     * 概述：封装存储空间
     * @param snCode 存储空间编码
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static byte[] storageName(long snCode) {
        return FSCode.LongToByte(snCode, 2); // 定长2字节
    }

    /**
     * 概述：封装uuid
     * @param uuid 唯一码
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static byte[] uuid(String uuid) {
        return FSCode.HexToByte(uuid);
    }

    /**
     * 概述：封装时间
     * @param time 时间,精确到分钟
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static byte[] time(long time) {
        return FSCode.LongToByte(time, 5); // 定长5字节
    }

    /**
     * 概述：封装ServerId
     * @param serverId 服务标识
     * @return
     * @throws Exception 
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static byte[] serverId(String serverId) throws Exception {
        String[] sidArr = serverId.split("_");
        byte[] sidBytes = null;
        for (String sidStr : sidArr) {
            int sid = Integer.valueOf(sidStr);
            byte[] tempArr = FSCode.moreFlagEncoder(sid, 7);
            if (sidBytes == null) {
                sidBytes = tempArr;
            } else {
                sidBytes = FSCode.addBytes(sidBytes, tempArr);
            }
        }
        return sidBytes;
    }

    /**
     * 概述：封装offset
     * @param offset
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static byte[] offset(long offset) {
        return FSCode.LongToByte(offset, 4); // 定长4字节
    }

    /**
     * 概述：封装size
     * @param size
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static byte[] size(long size) {
        return FSCode.LongToByte(size, 4); // 定长4字节
    }
}
