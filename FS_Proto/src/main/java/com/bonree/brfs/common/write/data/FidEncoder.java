package com.bonree.brfs.common.write.data;

import java.io.ByteArrayOutputStream;
import java.util.List;

import com.bonree.brfs.common.data.utils.Base64;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.proto.ReturnCodeProtos.ReturnCodeEnum;

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
    public static String buildOld(Fid fid) throws Exception {
        ReturnCodeEnum valicateCode = validate(fid);
        if (!ReturnCodeEnum.SUCCESS.equals(valicateCode)) {
            throw new Exception("Fid encoder failed! " + valicateCode);
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(FSCode.start);
        output.write(header(fid.getVersion(), fid.getCompress()));
        output.write(storageName(fid.getStorageNameCode()));
        output.write(uuid(fid.getUuid()));
        output.write(time(fid.getTime() / 1000 / 60));
        output.write(offset(fid.getOffset()));
        output.write(size(fid.getSize()));
        output.write(serverId(fid.getServerIdList()));
        output.write(FSCode.tail);
        output.write(new byte[]{0x0});
        output.write(duration(fid.getDuration()));
        // 封装fid
        return Base64.encodeToString(output.toByteArray(), Base64.DEFAULT).trim();
    }
    
    public static String build(Fid fid) {
    	return Base64.encodeToString(fid.toByteArray(), Base64.DEFAULT).replaceAll("\n", "");
    }

    /**
     * 概述：fid相关属性验证
     * @param fid
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static ReturnCodeEnum validate(Fid fid) {
        if (fid.getVersion() < 0 || fid.getVersion() > 7) { // version取值范围0~7
            return ReturnCodeEnum.FID_VERSION_ERROR;
        }
        if (fid.getCompress() < 0 || fid.getCompress() > 3) { // compress取值范围0~3
            return ReturnCodeEnum.FID_COMPRESS_ERROR;
        }
        if (fid.getStorageNameCode() < 0 || fid.getStorageNameCode() > 65535) { // storageNameCode取值范围0~65535
            return ReturnCodeEnum.FID_STORAGE_NAME_CODE_ERROR;
        }
        if (fid.getUuid() == null || fid.getUuid().length() > 32 || fid.getUuid().length() % 2 != 0) { // uuid长度为32字节
            return ReturnCodeEnum.FID_UUID_ERROR;
        }
        if (fid.getTime() <= 0 || fid.getTime() > 4701945540000L) { // time取值范围可到2118-12-31 23:59
            return ReturnCodeEnum.FID_TIME_ERROR;
        }
        if (fid.getServerIdCount() == 0) {
            return ReturnCodeEnum.FID_SERVERID_ERROR;
        } else {
            for (int sid : fid.getServerIdList()) {
                if (sid > 16383) { // serverId取值范围是0~16383
                    return ReturnCodeEnum.FID_SERVERID_ERROR;
                }
            }
        }
        if (fid.getOffset() < 0 || fid.getOffset() > 4294967295L) { // offset取值范围0~4294967295
            return ReturnCodeEnum.FID_OFFSET_ERROR;
        }
        if (fid.getSize() <= 0 || fid.getSize() > 4294967295L) { // size取值范围0~4294967295
            return ReturnCodeEnum.FID_SIZE_ERROR;
        }
        return ReturnCodeEnum.SUCCESS;
    }

    /**
     * 概述： 封装header
     * @param version 版本号
     * @param compress 压缩标识
     * @return
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static byte[] header(int version, int compress) {
        int v = version << 5;
        int c = compress << 3;
        return new byte[] { (byte) (v | c) };
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
    
    private static byte[] duration(String duration) {
    	return FSCode.StringToByte(duration);
    }

    /**
     * 概述：封装ServerId
     * @param fid 服务标识集合
     * @return
     * @throws Exception 
     * @user <a href=mailto:zhangnl@bonree.com>张念礼</a>
     */
    private static byte[] serverId(List<Integer> sidList) throws Exception {
        byte[] sidBytes = null;
        for (int sid : sidList) {
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
