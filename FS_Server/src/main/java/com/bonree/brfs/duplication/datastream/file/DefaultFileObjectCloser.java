package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.duplication.datastream.FilePathMaker;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSyncCallback;
import com.bonree.brfs.duplication.datastream.file.sync.FileObjectSynchronizer;
import com.bonree.brfs.duplication.filenode.FileNode;
import com.bonree.brfs.duplication.filenode.FileNodeStore;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManageLifecycle
public class DefaultFileObjectCloser implements FileObjectCloser, Closeable {
    private static final Logger log = LoggerFactory.getLogger(DefaultFileObjectCloser.class);

    private ExecutorService closeThreads;

    private FileObjectSynchronizer fileSynchronizer;
    private DiskNodeConnectionPool connectionPool;
    private FileNodeStore fileNodeStore;

    private FilePathMaker pathMaker;

    @Inject
    public DefaultFileObjectCloser(
        FileObjectSynchronizer fileSynchronizer,
        FileNodeStore fileNodeStore,
        DiskNodeConnectionPool connectionPool,
        FilePathMaker pathMaker) {
        this(Configs.getConfiguration().getConfig(RegionNodeConfigs.CONFIG_CLOSER_THREAD_NUM),
             fileSynchronizer,
             fileNodeStore,
             connectionPool,
             pathMaker);
    }

    public DefaultFileObjectCloser(int threadNum,
                                   FileObjectSynchronizer fileSynchronizer,
                                   FileNodeStore fileNodeStore,
                                   DiskNodeConnectionPool connectionPool,
                                   FilePathMaker pathMaker) {
        this.closeThreads = Executors.newFixedThreadPool(threadNum, new PooledThreadFactory("file_closer"));
        this.fileSynchronizer = fileSynchronizer;
        this.fileNodeStore = fileNodeStore;
        this.connectionPool = connectionPool;
        this.pathMaker = pathMaker;
    }

    @LifecycleStop
    @Override
    public void close() throws IOException {
        closeThreads.shutdown();
    }

    @Override
    public void close(FileObject file, boolean syncIfFailed) {
        log.debug("commit file[{}] to close", file.node().getName());
        closeThreads.submit(new CloseProcessor(file, syncIfFailed));
    }

    private class CloseProcessor implements Runnable {
        private final FileObject file;
        private final boolean syncIfNeeded;

        public CloseProcessor(FileObject file, boolean syncIfNeeded) {
            this.file = file;
            this.syncIfNeeded = syncIfNeeded;
        }

        private boolean closeDiskNodes() {
            boolean closeAll = true;
            FileNode fileNode = file.node();

            long closeCode = -1;
            log.debug("start to close file[{}]", fileNode.getName());
            for (DuplicateNode node : fileNode.getDuplicateNodes()) {
                DiskNodeConnection connection = connectionPool.getConnection(node.getGroup(), node.getId());
                if (connection == null || connection.getClient() == null) {
                    log.info("close error because node[{}] is disconnected!", node);
                    closeAll = false;
                    continue;
                }

                DiskNodeClient client = connection.getClient();
                String filePath = pathMaker.buildPath(fileNode, node);

                log.debug("closing file[{}] at node[{}]", filePath, node);
                long code = client.closeFile(filePath);
                log.debug("close file[{}] at node[{}] result[{}]", filePath, node, code);
                if (code < 0) {
                    closeAll = false;
                    continue;
                }

                if (closeCode == -1) {
                    closeCode = code;
                    continue;
                }

                if (closeCode != code) {
                    closeAll = false;
                }
            }

            return closeAll;
        }

        @Override
        public void run() {
            if (!closeDiskNodes() && syncIfNeeded) {
                fileSynchronizer.synchronize(file, new FileObjectSyncCallback() {

                    @Override
                    public void complete(FileObject file, long fileLength) {
                        log.info("final length is [{}] before close file[{}]", fileLength, file.node().getName());
                        close(file, true);
                    }

                    @Override
                    public void timeout(FileObject file) {
                        log.info("file[{}] is timeout to sync, close it");
                        close(file, false);
                    }
                });

                return;
            }

            try {
                fileNodeStore.delete(file.node().getName());
            } catch (Exception e) {
                log.error("delete file[{}] from file coordinator failed", file.node().getName());
            }
        }

    }
}
