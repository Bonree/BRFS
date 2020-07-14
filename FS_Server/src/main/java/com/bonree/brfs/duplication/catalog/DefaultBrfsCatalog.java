package com.bonree.brfs.duplication.catalog;

import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.common.rocksdb.WriteStatus;
import com.bonree.brfs.common.utils.Bytes;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBrfsCatalog implements BrfsCatalog {
    RocksDBManager rocksDBManager;
    static final byte[] DIR_VALUE = "0".getBytes();
    static final String SEPRATOR = "/";
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBrfsCatalog.class);
    private static final String pattern = "^(/+(\\.*[\\w,\\-]+\\.*)+)+$";
    private static Pattern p = Pattern.compile(pattern);
    private LoadingCache<PathKey, Boolean> pathCache = CacheBuilder.newBuilder()
                                                                   .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                                                                   .maximumSize(200)
                                                                   .initialCapacity(50)
                                                                   .expireAfterAccess(30, TimeUnit.SECONDS)
                                                                   .build(new CacheLoader<PathKey, Boolean>() {
                                                                       @SuppressWarnings("resource")
                                                                       @ParametersAreNonnullByDefault
                                                                       @Override
                                                                       public Boolean load(PathKey pathKey) {
                                                                           byte[] queryKey = transferToKey(pathKey.getPath());
                                                                           WriteStatus write;
                                                                           try {
                                                                               write = rocksDBManager
                                                                                   .write(pathKey.getSrName(), queryKey,
                                                                                          DIR_VALUE);
                                                                           } catch (Exception e) {
                                                                               LOG.error("error when add path[{}] to rocksdb!",
                                                                                         pathKey.getPath());
                                                                               throw new ProcessingException(
                                                                                   "error when add path");
                                                                           }
                                                                           if (write == WriteStatus.FAILED) {
                                                                               LOG.error("error when write path[{}] to rocksdb!",
                                                                                         pathKey.getPath());
                                                                               return false;
                                                                           }
                                                                           return true;
                                                                       }
                                                                   });

    /**
     * 把path转换为可以写入rocksdb的byte数组
     *
     * @param path 绝对路径
     *
     * @return （parent'hashcode/nodeName）的二进制数组
     */
    private byte[] transferToKey(String path) {
        String parentPath = getParentPath(path) + SEPRATOR;
        String nodeName = getLastNodeName(path);
        //        byte[] encode = encoder.encode(parentPath.getBytes());

        return Bytes.byteMerge(parentPath.getBytes(), nodeName.getBytes());
    }

    private String getParentPath(String path) {
        return path.substring(0, path.lastIndexOf("/"));
    }

    @Inject
    public DefaultBrfsCatalog(RocksDBManager rocksDBManager) {
        this.rocksDBManager = rocksDBManager;
    }

    @Override
    public boolean isUsable() {
        return rocksDBManager.isOpen();
    }

    @Override
    public List<String> getFidsByDir(String srName, String path) {
        if (!validPath(path)) {
            throw new NotFoundException();
        }

        LinkedList<String> fids = new LinkedList<>();
        byte[] prefixQueryKey;
        if ("/".equals(path)) {
            path = "";
        }
        prefixQueryKey = Bytes.byteMerge(path.getBytes(), "//".getBytes());
        Map<byte[], byte[]> map = rocksDBManager.readByPrefix(srName, prefixQueryKey);
        if (map == null) {
            LOG.error("dir [{}] is not found.", path);
            throw new NotFoundException();
        }

        String fid;
        String key;
        for (byte[] k : map.keySet()) {
            key = new String(k);
            //去掉自己
            if (new String(prefixQueryKey).equals(key)) {
                continue;
            }

            if (key.equals("//")) {
                continue;
            }
            String nodeName = getLastNodeNameWithOutSep(key);
            byte[] value = map.get(k);
            if (null == value) {
                String resp = "the path[" + path + "]'child[" + nodeName + "] is not store correctly";
                LOG.error(resp);
                throw new ServerErrorException(resp, Response.Status.NOT_FOUND);
            }
            fid = new String(value);
            if ("0".equals(fid)) {
                continue;
            }
            fids.add(fid);
        }
        return fids;
    }

    @Override
    public List<Inode> list(String srName, String path, int pageNo, int pageSize) {
        if (!validPath(path)) {
            throw new NotFoundException();
        }
        int startPos = (pageNo - 1) * pageSize;
        //int count = 0;
        ArrayList<Inode> inodes = new ArrayList<>(pageSize);
        byte[] prefixQueryKey;
        if ("/".equals(path)) {
            path = "";
        }
        prefixQueryKey = Bytes.byteMerge(path.getBytes(), "//".getBytes());
        Map<byte[], byte[]> map = rocksDBManager.readByPrefix(srName, prefixQueryKey, startPos, pageSize);
        if (map == null) {
            LOG.error("dir [{}] is not found.", path);
            throw new NotFoundException();
        }
        TreeMap<String, byte[]> treeMap = new TreeMap<>();
        for (byte[] bytes : map.keySet()) {
            treeMap.put(new String(bytes), map.get(bytes));
        }
        map.clear();
        for (String key : treeMap.keySet()) {
            //去掉自己
            if (new String(prefixQueryKey).equals(key)) {
                continue;
            }
            //if (count >= (startPos + pageSize)) {
            //    break;
            //}
            if (key.equals("//")) {
                continue;
            }
            String nodeName = getLastNodeNameWithOutSep(key);
            byte[] value = treeMap.get(key);
            if (null == value) {
                String resp = "the path[" + path + "]'child[" + nodeName + "] is not store correctly";
                LOG.error(resp);
                throw new ServerErrorException(resp, Response.Status.NOT_FOUND);
            }
            //if (count++ < startPos) {
            //    continue;
            //}
            if (new String(value).equals("0")) {
                inodes.add(new Inode(nodeName, null, 0));
            } else {
                inodes.add(new Inode(nodeName, new String(value), 1));
            }
        }

        return inodes;
    }

    @Override
    public boolean isFileNode(String srName, String path) {
        if (!validPath(path)) {
            throw new NotFoundException();
        }
        if ("/".equals(path)) {
            return false;
        }
        byte[] query = transferToKey(path);
        byte[] value = rocksDBManager.read(srName, query);
        if (value == null) {
            LOG.error("path [{}] is not found", path);
            throw new NotFoundException("path is not found!");
        }
        return !new String(value).equals("0");
    }

    @Override
    public boolean validPath(String path) {
        if ("/".equals(path)) {
            return true;
        }
        return p.matcher(path).matches();
    }

    /**
     * 写fid到rocksDB中，中间残缺目录将会被被创建
     *
     * @param srName storagename
     * @param path   绝对路径
     * @param fid    等待写入的fid
     *
     * @return 成功返回true 失败返回false
     */
    @Override
    public boolean writeFid(String srName, String path, String fid) {
        if (!validPath(path)) {
            LOG.error("invalid path : [{}]", path);
            return true;
        }
        byte[] key;
        try {
            createDirIfNeccessary(srName, path);
            //写文件
            key = transferToKey(path);
            LOG.debug("write fid[{}] of path[{}] of [{}]to rocksdb", fid, path, srName);
            WriteStatus write = rocksDBManager.write(srName, key, fid.getBytes(), true);
            if (write != WriteStatus.SUCCESS) {
                return true;
            }
        } catch (ProcessingException e) {
            LOG.error("get path [{}] Cache error", path);
            return true;
        } catch (Exception e) {
            LOG.error("Maybe its rocksDB can not write");
            return true;
        }
        return false;
    }

    /**
     * 检查一个路径的中间节点是否已经写入数据库，如果没有将会写入
     *
     * @param srName storage
     * @param path   绝对路径
     */
    private void createDirIfNeccessary(String srName, String path) throws Exception {
        String[] ancesstors = getAllAncesstors(path);
        StringBuilder grownPath = new StringBuilder();
        for (String ancesstor : ancesstors) {
            grownPath.append("/")
                     .append(ancesstor);
            pathCache.get(new PathKey(srName, grownPath.toString()));
        }
    }

    @Override
    public String getFid(String srName, String path) {
        byte[] query = transferToKey(path);
        byte[] value = rocksDBManager.read(srName, query);
        if (null == value) {
            String resp = "the path[" + path + "] is not not found";
            LOG.info(resp);
            throw new NotFoundException();
        }
        String s = new String(value);
        return s;
    }

    private String getLastNodeName(String path) {
        return path.substring(path.lastIndexOf("/"));
    }

    private String getLastNodeNameWithOutSep(String path) {
        int pos;
        if ((pos = path.lastIndexOf("/") + 1) >= path.length()) {
            LOG.error("the path [{}] is invalid!", path);
            throw new BadRequestException("the path [{}] is invalid!");
        }
        return path.substring(pos);
    }

    public String[] getAllAncesstors(String absPath) {
        if (isTopAncesstor(absPath)) {
            return new String[] {""};
        }
        String substring = absPath.substring(1, absPath.lastIndexOf("/"));
        return substring.split("/");
    }

    private boolean isTopAncesstor(String path) {
        return path.lastIndexOf("/") == 0;
    }

    private static class PathKey {
        private String srName;
        private String path;

        public PathKey(String srName, String path) {
            this.srName = srName;
            this.path = path;
        }

        public String getSrName() {
            return srName;
        }

        public void setSrName(String srName) {
            this.srName = srName;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PathKey pathKey = (PathKey) o;
            return srName.equals(pathKey.srName)
                && path.equals(pathKey.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(srName, path);
        }
    }
}
