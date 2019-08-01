/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltcore.zk.ZKUtil.ByteArrayCallback;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * Tracker monitors and provides snapshots of a single ZK node's
 * children. The children data objects must be JSONObjects.
 */
public class SingleNodeLeaderCache {

    // HSID for test only
    public static long TEST_LAST_HSID = Long.MAX_VALUE -1;
    protected final ZooKeeper m_zk;
    private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
    protected final Callback m_cb; // the callback when the cache changes
    private final String m_whoami; // identify the owner of the leader cache
    private final int m_partitionId;

    // the children of this node are observed.
    protected final String m_rootNode;

    // All watch processing is run serially in this thread.
    private final ListeningExecutorService m_es;

    // the cache exposed to the public. Start empty. Love it.
    protected volatile LeaderCallBackInfo m_publicCache = null;


    public static final String migrate_partition_leader_suffix = "_migrated";

    public static class LeaderCallBackInfo {
        Long m_lastHSId;
        Long m_HSId;
        boolean m_isMigratePartitionLeaderRequested;

        public LeaderCallBackInfo(Long lastHSId, Long HSId, boolean isRequested) {
            m_lastHSId = lastHSId;
            m_HSId = HSId;
            m_isMigratePartitionLeaderRequested = isRequested;
        }

        @Override
        public String toString() {
            return "leader hsid: " + CoreUtils.hsIdToString(m_HSId) +
                    ( m_lastHSId != Long.MAX_VALUE ? " (previously " + CoreUtils.hsIdToString(m_lastHSId) + ")" : "" ) +
                    ( m_isMigratePartitionLeaderRequested ? ", MigratePartitionLeader requested" : "");
        }
    }

    /**
     * Generate a HSID string with BALANCE_SPI_SUFFIX information.
     * When this string is updated, we can tell the reason why HSID is changed.
     */
    public static String suffixHSIdsWithMigratePartitionLeaderRequest(Long HSId) {
        return Long.toString(Long.MAX_VALUE) + "/" + Long.toString(HSId) + migrate_partition_leader_suffix;
    }

    /**
     * Is the data string hsid written because of MigratePartitionLeader request?
     */
    public static boolean isHSIdFromMigratePartitionLeaderRequest(String HSIdInfo) {
        return HSIdInfo.endsWith(migrate_partition_leader_suffix);
    }

    public static LeaderCallBackInfo buildLeaderCallbackFromString(String HSIdInfo) {
        int nextHSIdOffset = HSIdInfo.indexOf("/");
        assert(nextHSIdOffset >= 0);
        long lastHSId = Long.parseLong(HSIdInfo.substring(0, nextHSIdOffset));
        boolean migratePartitionLeader = isHSIdFromMigratePartitionLeaderRequest(HSIdInfo);
        long nextHSId;
        if (migratePartitionLeader) {
            nextHSId = Long.parseLong(HSIdInfo.substring(nextHSIdOffset+1, HSIdInfo.length() - migrate_partition_leader_suffix.length()));
        }
        else {
            nextHSId = Long.parseLong(HSIdInfo.substring(nextHSIdOffset+1));
        }
        return new LeaderCallBackInfo(lastHSId, nextHSId, migratePartitionLeader);
    }

    /**
     * Callback is passed an immutable cache when a child (dis)appears/changes.
     * Callback runs in the LeaderCache's ES (not the zk trigger thread).
     */
    public abstract static class Callback
    {
        abstract public void run(LeaderCallBackInfo cache);
    }

    public SingleNodeLeaderCache(ZooKeeper zk, String from, String rootNode, int pid)
    {
        this(zk, from, rootNode, pid, null);
    }

    public SingleNodeLeaderCache(ZooKeeper zk, String from, String rootNode, int pid, Callback cb)
    {
        m_zk = zk;
        m_whoami = from;
        m_rootNode = rootNode;
        m_partitionId = pid;
        m_cb = cb;
        m_es = CoreUtils.getCachedSingleThreadExecutor("LeaderCache-" + m_whoami, 15000);
    }

    /** Initialize and start watching the cache. */
    public void start(boolean block) throws InterruptedException, ExecutionException {
        Future<?> task = m_es.submit(new ParentEvent(null));
        if (block) {
            task.get();
        }
    }

    /**
     * Initialized and start watching partition level cache, this function is blocking.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void startPartitionWatch() throws InterruptedException, ExecutionException {
        Future<?> task = m_es.submit(new PartitionWatchEvent());
        task.get();
    }

    /** Stop caring */
    public void shutdown() throws InterruptedException {
        m_shutdown.set(true);
        m_es.shutdown();
        m_es.awaitTermination(356, TimeUnit.DAYS);
    }

    /**
     * Get a current snapshot of the watched partition node. This snapshot
     * promises no cross-children atomicity guarantees.
     */
    public Pair<Integer, Long> pointInTimeCache() {
        if (m_shutdown.get()) {
            throw new RuntimeException("Requested cache from shutdown LeaderCache.");
        }
        // Avoid race between atomic change of m_publicCache change.
        LeaderCallBackInfo cacheCopy = m_publicCache;
        return Pair.of(m_partitionId, cacheCopy.m_HSId);
    }

    /**
     * Read a single key from the cache. Matches the semantics of put()
     */
    public Long get() {
        LeaderCallBackInfo info = m_publicCache;
        if (info == null) {
            return null;
        }

        return info.m_HSId;
    }

    public boolean isMigratePartitionLeaderRequested() {
        LeaderCallBackInfo info = m_publicCache;
        if (info != null) {
            return info.m_isMigratePartitionLeaderRequested;
        }
        return false;
    }

    // parent (root node) sees new or deleted child
    private class ParentEvent implements Runnable {
        public ParentEvent(WatchedEvent event) {
        }

        @Override
        public void run() {
            try {
                processParentEvent();
            } catch (Exception e) {
                // ignore post-shutdown session termination exceptions.
                if (!m_shutdown.get()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected failure in LeaderCache.", true, e);
                }
            }
        }
    }

    // Boilerplate to forward zookeeper watches to the executor service
    protected final Watcher m_parentWatch = new Watcher() {
        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_shutdown.get()) {
                    m_es.submit(new ParentEvent(event));
                }
            } catch (RejectedExecutionException e) {
                if (!m_es.isShutdown()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected rejected execution exception", false, e);
                }
            }
        }
    };

    /**
     * Rebuild the point-in-time snapshot of the partitionId child we are tracking
     * and set watches on new children.
     */
    protected void processParentEvent() throws Exception {
        // get current children snapshot and reset this watch.
        if (m_publicCache == null) {
            List<String> children = m_zk.getChildren(m_rootNode, m_parentWatch);
            String targetPartition = Integer.toString(m_partitionId);
            if (children.contains(targetPartition)) {
                ByteArrayCallback cb = new ByteArrayCallback();
                m_zk.getData(ZKUtil.joinZKPath(m_rootNode, targetPartition), m_childWatch, cb, null);
                try {
                    byte payload[] = cb.get();
                    // During initialization children node may contain no data.
                    if (payload != null) {
                        String data = new String(payload, "UTF-8");
                        LeaderCallBackInfo info = SingleNodeLeaderCache.buildLeaderCallbackFromString(data);
                        m_publicCache = info;
                        if (m_cb != null) {
                            m_cb.run(m_publicCache);
                        }
                    }
                } catch (KeeperException.NoNodeException e) {
                    // child may have been deleted between the parent trigger and getData.
                }
            }
        }
        // if m_parentWatch fires after we have applied m_publicCache, we will ignore the event causing
        // the root node to stop being watched.
    }

    // child node sees modification or deletion
    private class ChildEvent implements Runnable {
        private final WatchedEvent m_event;
        public ChildEvent(WatchedEvent event) {
            m_event = event;
        }

        @Override
        public void run() {
            try {
                processChildEvent(m_event);
            } catch (Exception e) {
                // ignore post-shutdown session termination exceptions.
                if (!m_shutdown.get()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected failure in LeaderCache.", true, e);
                }
            }
        }
    }

    // Boilerplate to forward zookeeper watches to the executor service
    protected final Watcher m_childWatch = new Watcher() {
        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_shutdown.get()) {
                    m_es.submit(new ChildEvent(event));
                }
            } catch (RejectedExecutionException e) {
                if (!m_es.isShutdown()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected rejected execution exception", false, e);
                }
            }
        }
    };

    /**
     * Update a modified child and republish a new snapshot. This may indicate
     * a deleted child or a child with modified data.
     */
    private void processChildEvent(WatchedEvent event) throws Exception {
        ByteArrayCallback cb = new ByteArrayCallback();
        m_zk.getData(event.getPath(), m_childWatch, cb, null);
        try {
            // cb.getData() and cb.getPath() throw KeeperException
            byte payload[] = cb.get();
            String data = new String(payload, "UTF-8");
            m_publicCache = SingleNodeLeaderCache.buildLeaderCallbackFromString(data);
            assert(m_partitionId == getPartitionIdFromZKPath(cb.getPath()));

        } catch (KeeperException.NoNodeException e) {
            // rtb: I think result's path is the same as cb.getPath()?
            assert(m_partitionId == getPartitionIdFromZKPath(cb.getPath()));
            m_publicCache = null;
        }
        if (m_cb != null) {
            m_cb.run(m_publicCache);
        }
    }

    // parent (root node) sees new or deleted child
    private class PartitionWatchEvent implements Runnable {
        public PartitionWatchEvent() {
        }

        @Override
        public void run() {
            try {
                processPartitionWatchEvent();
            } catch (Exception e) {
                // ignore post-shutdown session termination exceptions.
                if (!m_shutdown.get()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected failure in LeaderCache.", true, e);
                }
            }
        }
    }

    // Race to create partition-specific zk node and put a watch on it.
    private void processPartitionWatchEvent() throws KeeperException, InterruptedException {
        try {
            m_zk.create(m_rootNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            m_zk.getData(m_rootNode, m_childWatch, null);
        } catch (KeeperException.NodeExistsException e) {
            m_zk.getData(m_rootNode, m_childWatch, null);
        }
    }

    // example zkPath string: /db/iv2masters/1
    protected static int getPartitionIdFromZKPath(String zkPath)
    {
        return Integer.parseInt(zkPath.substring(zkPath.lastIndexOf('/') + 1));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LeaderCache, root node:").append(m_rootNode).append("\n");
        sb.append("public cache: partition id -> HSId -> isMigratePartitionLeader\n");
        sb.append("             ").append(m_partitionId).append(" -> ").append(m_publicCache).append("\n");

        return sb.toString();
    }
}
