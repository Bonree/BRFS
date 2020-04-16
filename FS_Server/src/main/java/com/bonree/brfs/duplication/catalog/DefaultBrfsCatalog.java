package com.bonree.brfs.duplication.catalog;

import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.common.rocksdb.WriteStatus;
import com.bonree.brfs.common.utils.Bytes;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultBrfsCatalog implements BrfsCatalog {
    RocksDBManager rocksDBManager;
    public static final byte[] rootID = "0".getBytes();
    AtomicLong idGen = new AtomicLong(0l);
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBrfsCatalog.class);

    @Inject
    public DefaultBrfsCatalog(RocksDBManager rocksDBManager) {
        this.rocksDBManager = rocksDBManager;
    }

    @Override
    public boolean isUsable() {
        return rocksDBManager.isOpen();
    }

    @Override
    public List<Inode> list(String srName, String path, int pageNo, int pageSize) {
        int startPos = (pageNo-1) * pageSize;
        int endPos = startPos + pageSize;
        ArrayList<Inode> inodes = new ArrayList<>(pageSize);

        return null;
    }

    @Override
    public boolean isFileNode(String srName, String path) {
        String[] ancesstors = getAllAncesstors(path);
        StringBuilder pathSb = new StringBuilder("");
        byte[] parentID = rootID;
        for (String s : ancesstors) {
            parentID = rocksDBManager.read(srName, Bytes.byteMerge(parentID,("/"+s).getBytes()));
        }
        return false;
    }

    /**
     * 写fid到rocksDB中，中间残缺目录将会被被创建
     * @param srName storagename
     * @param path
     * @param fid
     * @return 成功返回true 失败返回false
     * @throws Exception 如果写rocksDB失败将会抛出这个错误
     */
    @Override
    public boolean writeFid(String srName, String path, String fid) {
        String[] ancesstors = getAllAncesstors(path);
        byte[] parentID = rootID;
        byte[] queryKey;
        InodeValue readValue;
        try {
            for (String ancesstor : ancesstors) {
                queryKey = Bytes.byteMerge(parentID,("/"+ancesstor).getBytes());
                byte[] value = rocksDBManager.read(srName, queryKey);
                readValue = InodeValue.deSerialize(value);
                parentID = readValue.getInodeID();
                if(parentID == null){
                    long id = idGen.incrementAndGet();
                    byte[] writeValue = new InodeValue()
                            .setID(id)
                            .build()
                            .toByteArray();
                    WriteStatus writeStatus = rocksDBManager.write(srName, queryKey, writeValue);
                    if(writeStatus != WriteStatus.SUCCESS){
                        return false;
                    }
                    parentID = Bytes.long2Bytes(id);
                }
            }
            String lastNodeName = getLastNodeName(path);
            WriteStatus write = rocksDBManager.write(srName, Bytes.byteMerge(parentID, ("/" + lastNodeName).getBytes()), Bytes.long2Bytes(idGen.incrementAndGet()));
            if(write != WriteStatus.SUCCESS){
                return false;
            }
        } catch (InvalidProtocolBufferException e) {
            LOG.error("deserialize error!{}",e);
            return false;
        } catch (Exception e) {
            LOG.error("Maybe its rocksDB can not write {}",e);
            return false;
        }
        return true;
    }

    @Override
    public String getFid(String srName, String path) {
        String[] ancesstors = getAllAncesstors(path);
        byte[] parentID = rootID;
        for (String s : ancesstors) {
            parentID = rocksDBManager.read(srName, Bytes.byteMerge(parentID,("/"+s).getBytes()));
            if(parentID == null){
                return null;
            }
        }
        String lastNodeName = getLastNodeName(path);
        byte[] read = rocksDBManager.read(srName, Bytes.byteMerge(parentID, lastNodeName.getBytes()));
        return null;
    }

    public boolean isExist(String srName, String path){
        String[] ancesstors = getAllAncesstors(path);
        byte[] parentID = rootID;
        for (String s : ancesstors) {
            parentID = rocksDBManager.read(srName, Bytes.byteMerge(parentID,("/"+s).getBytes()));
            if(parentID == null){
                return false;
            }
        }
        String lastNodeName = getLastNodeName(path);
        return rocksDBManager.read(srName,Bytes.byteMerge(parentID, lastNodeName.getBytes())) != null;
    }

    private String getLastNodeName(String path) {
        return path.substring(path.lastIndexOf("/"));
    }

    public String[] getAllAncesstors(String absPath){
        String substring = absPath.substring(1,absPath.lastIndexOf("/"));
        return substring.split("/");
    }
    public static void main(String[] args) {
        String s = "/1/2/3/4";
        String[] split = s.split("/");
        System.out.println(split.length);
        System.out.println(s.substring(1,s.lastIndexOf("/")));
        System.out.println(new DefaultBrfsCatalog(null).getAllAncesstors(s).length);
    }
}
