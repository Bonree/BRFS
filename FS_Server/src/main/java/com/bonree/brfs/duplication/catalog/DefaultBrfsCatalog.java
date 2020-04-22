package com.bonree.brfs.duplication.catalog;

import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.common.rocksdb.WriteStatus;
import com.bonree.brfs.common.utils.Bytes;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class DefaultBrfsCatalog implements BrfsCatalog {
    RocksDBManager rocksDBManager;
    public static final byte[] rootID = "0".getBytes();
//    AtomicLong idGen = new AtomicLong(0l);
    static final Base64.Decoder decoder = Base64.getDecoder();
    static final Base64.Encoder encoder = Base64.getEncoder();
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBrfsCatalog.class);
    private static final String pattern = "^\\/(\\.*[\\w,\\-]+\\.*\\/?)+$";

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
        ArrayList<Inode> inodes = new ArrayList<>(pageSize);
        String[] allAncesstors = getAllAncesstors(path);
        byte[] parentID = rootID;
        for (String ancesstor : allAncesstors) {
            parentID = rocksDBManager.read(srName, Bytes.byteMerge(parentID,("/"+ancesstor).getBytes()));
            if (parentID == null){
                throw new NotFoundException("the path :"+path+" is not exsit");
            }
        }

        String lastNodeName = getLastNodeName(path);
        byte[] read = rocksDBManager.read(srName, Bytes.byteMerge(parentID, lastNodeName.getBytes()));
        Map<byte[], byte[]> map = rocksDBManager.readByPrefix(srName, read);
        for (byte[] key : map.keySet()) {
            String nodeName = getLastNodeName(new String(key));
            byte[] value = map.get(key);
            if(null == value){
                String resp = "the path["+path+"]'child["+nodeName+"] is not store correctly";
                LOG.error(resp);
                throw new ServerErrorException(resp, Response.Status.NOT_FOUND);
            }
            try {
                //todo work suspend here
                InodeValue inodeValue = InodeValue.deSerialize(value);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("deserialize error!");
                throw new ProcessingException("deserialize error!");
            }
        }

        return null;
    }

    @Override
    public boolean isFileNode(String srName, String path) {
        String[] ancesstors = getAllAncesstors(path);
        StringBuilder pathSb = new StringBuilder("");
        byte[] parentID = rootID;
        for (String s : ancesstors) {
            parentID = rocksDBManager.read(srName, Bytes.byteMerge(parentID,("/"+s).getBytes()));
            if (parentID == null){
                //todo 不存在
                return false;
            }
        }
        return false;
    }
    @Override
    public boolean validPath(String path){
        return Pattern.matches(pattern,path);
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
        if(!validPath(path)){
            LOG.error("invalid path : [{}]",path);
            return false;
        }
        String[] ancesstors = getAllAncesstors(path);
        byte[] parentID = rootID;
        byte[] queryKey;
        InodeValue readValue;
        try {
            for (String ancesstor : ancesstors) {
                queryKey = Bytes.byteMerge(parentID,("/"+ancesstor).getBytes());
                byte[] value = rocksDBManager.read(srName, queryKey);
                String id;
                if(value == null){
                    id = creatDir(srName , queryKey);
                    if(id == null){
                        LOG.error("error when create dir [{}]",new String(queryKey));
                        return false;
                    }
                    parentID = id.getBytes();
                }else {
                    readValue = InodeValue.deSerialize(value);
                    parentID = readValue.getInodeID();
                }
            }
            String lastNodeName = getLastNodeName(path);
            byte[] fidBytes = new InodeValue().setFid(fid).build().toByteArray();
            WriteStatus write = rocksDBManager.write(srName, Bytes.byteMerge(parentID, lastNodeName.getBytes()), fidBytes);
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

    /**
     * 生成一个目录，并赋值（全局唯一递增）
     * @param srName
     * @param queryKey 父id + dir名
     * @return
     */
    private String creatDir(String srName, byte[] queryKey) throws Exception {
        String id = encoder.encodeToString(queryKey);
        byte[] writeValue = new InodeValue()
                .setID(id)
                .build()
                .toByteArray();
        WriteStatus writeStatus = rocksDBManager.write(srName, queryKey, writeValue);
        if(writeStatus != WriteStatus.SUCCESS){
            return null;
        }
        return id;
    }
    @Override
    public String getFid(String srName, String path) {
        String[] ancesstors = getAllAncesstors(path);
        byte[] parentID = rootID;
        for (String s : ancesstors) {
            parentID = rocksDBManager.read(srName, Bytes.byteMerge(parentID,("/"+s).getBytes()));
            if(parentID == null){
                LOG.info("the path [{}]does not exsit in the rocksdb ",path);
                return null;
            }
            try {
                parentID = InodeValue.deSerialize(parentID).getInodeID();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("deserialize error when get fid!");
                e.printStackTrace();
            }
        }
        String lastNodeName = getLastNodeName(path);
        byte[] read = rocksDBManager.read(srName, Bytes.byteMerge(parentID, lastNodeName.getBytes()));
        InodeValue inodeValue;
        if(read != null){
            try {
                inodeValue = InodeValue.deSerialize(read);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("deSerialize error when get fid");
                return null;
            }
            return inodeValue.getFid();
        }
        LOG.info("the path [{}]does not exsit in the rocksdb ",path);
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
