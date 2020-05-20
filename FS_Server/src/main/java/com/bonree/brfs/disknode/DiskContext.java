package com.bonree.brfs.disknode;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import com.bonree.brfs.duplication.filenode.FilePathBuilder;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.PartitionInterface;
import java.io.File;
import java.util.List;
import javax.inject.Inject;

public class DiskContext {
    public static final String URI_PING_PONG_ROOT = "/ping";
    public static final String URI_DISK_NODE_ROOT = "/disk";
    public static final String URI_FLUSH_NODE_ROOT = "/flush";
    public static final String URI_LENGTH_NODE_ROOT = "fileLength";
    public static final String URI_COPY_NODE_ROOT = "/copy";
    public static final String URI_LIST_NODE_ROOT = "/list";
    public static final String URI_META_NODE_ROOT = "/metadata";
    public static final String URI_RECOVER_NODE_ROOT = "/recover";

    private final List<String> storageDirs;
    private final PartitionInterface partition;
    private final StorageRegionManager storageRegionManager;

    @Inject
    public DiskContext(
        StorageConfig config,
        PartitionInterface partition,
        StorageRegionManager storageRegionManager) {
        this(config.getStorageDirs(), partition, storageRegionManager);
    }

    public DiskContext(
        List<String> storageDirs,
        PartitionInterface partition,
        StorageRegionManager storageRegionManager) {
        this.storageDirs = requireNonNull(storageDirs)
            .stream()
            .map(dir -> {
                File d = new File(dir);
                if (!d.exists()) {
                    d.mkdirs();
                }

                return d;
            })
            .map(File::getAbsolutePath)
            .collect(toImmutableList());
        this.partition = partition;
        this.storageRegionManager = storageRegionManager;
    }

    public List<String> getStorageDirs() {
        return this.storageDirs;
    }

    private String getStorageDir(String filePath) {
        return partition.getDataDirByPath(filePath);
    }

    /**
     * 从用户使用的逻辑路径转换为实际磁盘中的真实路径
     *
     * @param logicPath
     *
     * @return
     */
    public String getConcreteFilePath(String logicPath) {
        return new File(getStorageDir(logicPath), logicPath).getAbsolutePath();
    }

    /**
     * 从磁盘上的真实路径转换为用户使用的逻辑路径
     *
     * @param path
     *
     * @return
     */
    public String getLogicFilePath(String path) {
        for (String dir : storageDirs) {
            if (path.startsWith(dir)) {
                return path.substring(dir.length());
            }
        }

        throw new IllegalArgumentException("path[" + path + "] isn't illegal real path");
    }
}
