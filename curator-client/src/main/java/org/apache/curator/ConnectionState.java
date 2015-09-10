/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.curator;

import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.drivers.TracerDriver;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.utils.DebugUtils;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class ConnectionState implements Watcher, Closeable {
    private static final int MAX_BACKGROUND_EXCEPTIONS = 10;
    private static final boolean LOG_EVENTS = Boolean.getBoolean(DebugUtils.PROPERTY_LOG_EVENTS);
    private static final Logger log = LoggerFactory.getLogger(ConnectionState.class);
    private final HandleHolder zooKeeper;
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final EnsembleProvider ensembleProvider;
    private final int sessionTimeoutMs;
    private final int connectionTimeoutMs;
    private final AtomicReference<TracerDriver> tracer;
    private final Queue<Exception> backgroundExceptions = new ConcurrentLinkedQueue<Exception>();
    private final Queue<Watcher> parentWatchers = new ConcurrentLinkedQueue<Watcher>();
    private final AtomicLong instanceIndex = new AtomicLong();
    private volatile long connectionStartMs = 0;

    ConnectionState(ZookeeperFactory zookeeperFactory, EnsembleProvider ensembleProvider, int sessionTimeoutMs, int connectionTimeoutMs, Watcher parentWatcher, AtomicReference<TracerDriver> tracer, boolean canBeReadOnly) {
        this.ensembleProvider = ensembleProvider;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.tracer = tracer;

        if (parentWatcher != null) {
            parentWatchers.offer(parentWatcher);
        }

        zooKeeper = new HandleHolder(zookeeperFactory, this, ensembleProvider, sessionTimeoutMs, canBeReadOnly);
    }

    ZooKeeper getZooKeeper() throws Exception {
        if (SessionFailRetryLoop.sessionForThreadHasFailed()) {
            throw new SessionFailRetryLoop.SessionFailedException();
        }

        Exception exception = backgroundExceptions.poll();
        if (exception != null) {
            tracer.get().addCount("background-exceptions", 1);
            throw exception;
        }

        // 如果没有连接上
        boolean localIsConnected = isConnected.get();
        if (!localIsConnected) {
            checkTimeouts();
        }

        // 需要先 start
        return zooKeeper.getZooKeeper();
    }

    boolean isConnected() {
        return isConnected.get();
    }

    void start() throws Exception {
        log.debug("Starting");
        ensembleProvider.start(); // 忽略
        reset();
    }

    @Override
    public void close() throws IOException {
        log.debug("Closing");

        CloseableUtils.closeQuietly(ensembleProvider); // 忽略
        try {
            zooKeeper.closeAndClear();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            isConnected.set(false);
        }
    }

    void addParentWatcher(Watcher watcher) {
        parentWatchers.offer(watcher);
    }

    void removeParentWatcher(Watcher watcher) {
        parentWatchers.remove(watcher);
    }

    long getInstanceIndex() {
        return instanceIndex.get();
    }

    @Override
    public void process(WatchedEvent event) {
        if (LOG_EVENTS) {
            log.debug("ConnectState watcher: " + event);
        }

        if (event.getType() == Watcher.Event.EventType.None) {
            boolean wasConnected = isConnected.get();
            boolean newIsConnected = checkState(event.getState(), wasConnected);

            // 连接状态发生改变
            if (newIsConnected != wasConnected) {
                isConnected.set(newIsConnected);
                connectionStartMs = System.currentTimeMillis();
            }
        }

        // parentWatcher处理Event
        // 大致的逻辑:
        // CuratorFramework 能保证任何操作可靠地执行到Zookeeper上，但是如果出现Session Expire等，则将之前的ZK Close Reset
        for (Watcher parentWatcher : parentWatchers) {
            TimeTrace timeTrace = new TimeTrace("connection-state-parent-process", tracer.get());
            parentWatcher.process(event);
            timeTrace.commit();
        }
    }

    EnsembleProvider getEnsembleProvider() {
        return ensembleProvider;
    }

    // 如果没有连接上， 则需要考虑: 开始准备连接到现在的时间间隔，如果太长，则直接抛出Exception
    private synchronized void checkTimeouts() throws Exception {
        int minTimeout = Math.min(sessionTimeoutMs, connectionTimeoutMs);
        long elapsed = System.currentTimeMillis() - connectionStartMs;
        if (elapsed >= minTimeout) {
            if (zooKeeper.hasNewConnectionString()) {
                handleNewConnectionString();
            } else {
                int maxTimeout = Math.max(sessionTimeoutMs, connectionTimeoutMs);
                if (elapsed > maxTimeout) {
                    if (!Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES)) {
                        log.warn(String.format("Connection attempt unsuccessful after %d (greater than max timeout of %d). Resetting connection and trying again with a new connection.", elapsed, maxTimeout));
                    }
                    reset();
                } else {
                    KeeperException.ConnectionLossException connectionLossException = new CuratorConnectionLossException();
                    if (!Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES)) {
                        log.error(String.format("Connection timed out for connection string (%s) and timeout (%d) / elapsed (%d)", zooKeeper.getConnectionString(), connectionTimeoutMs, elapsed), connectionLossException);
                    }
                    tracer.get().addCount("connections-timed-out", 1);
                    throw connectionLossException;
                }
            }
        }
    }


    /**
     * 关闭现有的zk, 重新创建一个新的
     */
    private synchronized void reset() throws Exception {
        log.debug("reset");

        instanceIndex.incrementAndGet();

        isConnected.set(false);
        connectionStartMs = System.currentTimeMillis();

        // 清除之前的状态
        zooKeeper.closeAndReset();
        // 创建zk
        zooKeeper.getZooKeeper();   // initiate connection
    }

    /**
     * 状态如何变迁呢?
     *
     * @param state
     * @param wasConnected
     * @return
     */
    private boolean checkState(Event.KeeperState state, boolean wasConnected) {
        // 新状态：
        //         默认为当前状态
        //         除非发生了某些关键的State变化，其他情况下都不会有连接状态的改变
        //
        boolean isConnected = wasConnected;
        boolean checkNewConnectionString = true;
        switch (state) {
            default:
            case Disconnected: {
                isConnected = false;
                break;
            }

            case SyncConnected:
            case ConnectedReadOnly: {
                isConnected = true;
                break;
            }

            case AuthFailed: {
                isConnected = false;
                log.error("Authentication failed");
                break;
            }

            case Expired: {
                isConnected = false;
                checkNewConnectionString = false;

                // 如何处理Expired呢?
                handleExpiredSession();
                break;
            }

            case SaslAuthenticated: {
                // NOP
                break;
            }
        }

        // 暂不考虑这个逻辑
        if (checkNewConnectionString && zooKeeper.hasNewConnectionString()) {
            handleNewConnectionString();
        }

        return isConnected;
    }

    // 日常使用暂不考虑
    private void handleNewConnectionString() {
        log.info("Connection string changed");
        tracer.get().addCount("connection-string-changed", 1);

        try {
            reset();
        } catch (Exception e) {
            queueBackgroundException(e);
        }
    }

    /**
     * 过期了，也会RESET
     */
    private void handleExpiredSession() {
        log.warn("Session expired event received");
        tracer.get().addCount("session-expired", 1);

        try {
            reset();
        } catch (Exception e) {
            queueBackgroundException(e);
        }
    }

    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored"})
    private void queueBackgroundException(Exception e) {
        while (backgroundExceptions.size() >= MAX_BACKGROUND_EXCEPTIONS) {
            backgroundExceptions.poll();
        }
        backgroundExceptions.offer(e);
    }
}
