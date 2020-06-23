package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuplicateNodeChecker {
    private static final Logger log = LoggerFactory.getLogger(DuplicateNodeChecker.class);

    private final DiskNodeConnectionPool connectionPool;
    private final ServiceManager serviceManager;

    private final ScheduledExecutorService checkExec = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "node_checker");
            t.setDaemon(true);

            return t;
        }
    });

    private final Set<DuplicateNode> checkingNodes = new TreeSet<>(new Comparator<DuplicateNode>() {
        @Override
        public int compare(DuplicateNode o1, DuplicateNode o2) {
            int groupDiff = o1.getGroup().compareTo(o2.getGroup());
            if (groupDiff != 0) {
                return groupDiff;
            }

            return o1.getId().compareTo(o2.getId());
        }
    });

    @Inject
    public DuplicateNodeChecker(DiskNodeConnectionPool connectionPool,
                                ServiceManager serviceManager) {
        this.connectionPool = connectionPool;
        this.serviceManager = serviceManager;
    }


    @LifecycleStart
    public void start() {
        log.info("start duplicate node checker");
        checkExec.scheduleWithFixedDelay(() -> {
            for (DuplicateNode node : ImmutableList.copyOf(checkingNodes)) {
                if (serviceManager.getServiceById(node.getGroup(), node.getId()) == null) {
                    checkingNodes.remove(node);
                    continue;
                }

                checkNode(node);
            }
        }, 5, 10, TimeUnit.SECONDS);
    }

    @LifecycleStop
    public void stop() {
        log.info("stop duplicate node checker");
        checkExec.shutdown();
    }

    public DuplicateNode[] filterCorruptNode(DuplicateNode[] nodes) {
        List<DuplicateNode> result = new LinkedList<>();
        for (DuplicateNode node : nodes) {
            if (checkingNodes.contains(node)) {
                continue;
            }

            result.add(node);
        }

        return result.toArray(new DuplicateNode[result.size()]);
    }

    public void checkNode(DuplicateNode node) {
        log.info("checking node {}", node);
        DiskNodeConnection connection = connectionPool.getConnection(node.getGroup(), node.getId());
        if (connection == null) {
            checkingNodes.add(node);
            return;
        }

        if (!connection.getClient().ping()) {
            checkingNodes.add(node);

            try {
                connection.close();
            } catch (IOException e) {
                log.warn("close connection of {} error", node, e);
            }

            return;
        }

        checkingNodes.remove(node);
    }

}
