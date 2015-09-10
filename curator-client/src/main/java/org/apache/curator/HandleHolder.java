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

import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 持有zk, closeReset, closeClear
 */
class HandleHolder {
    private final ZookeeperFactory zookeeperFactory;
    private final Watcher watcher;
    private final EnsembleProvider ensembleProvider; // 就当做一个简单的字符串使用: Connection String
    private final int sessionTimeout;
    private final boolean canBeReadOnly;

    private volatile Helper helper;

    private interface Helper {
        ZooKeeper getZooKeeper() throws Exception;

        String getConnectionString();
    }

    HandleHolder(ZookeeperFactory zookeeperFactory, Watcher watcher, EnsembleProvider ensembleProvider, int sessionTimeout, boolean canBeReadOnly) {
        this.zookeeperFactory = zookeeperFactory;
        this.watcher = watcher;
        this.ensembleProvider = ensembleProvider;
        this.sessionTimeout = sessionTimeout;
        this.canBeReadOnly = canBeReadOnly;
    }

    ZooKeeper getZooKeeper() throws Exception {
        return (helper != null) ? helper.getZooKeeper() : null;
    }

    String getConnectionString() {
        return (helper != null) ? helper.getConnectionString() : null;
    }

    // 可认为总是返回 false
    boolean hasNewConnectionString() {
        // helper中记录的，和ensembleProvider中记录的不一致时，就是出现新的Connection String
        String helperConnectionString = (helper != null) ? helper.getConnectionString() : null;
        return (helperConnectionString != null) && !ensembleProvider.getConnectionString().equals(helperConnectionString);
    }

    void closeAndClear() throws Exception {
        internalClose();
        helper = null;
    }

    // 启动的动作: closeAndReset
    void closeAndReset() throws Exception {
        internalClose();

        // first helper is synchronized when getZooKeeper is called. Subsequent calls
        // are not synchronized.
        // 语义上就是通过Helper获取一个Zookeeper, 可能有异常
        helper = new Helper() {
            private volatile ZooKeeper zooKeeperHandle = null;
            private volatile String connectionString = null;


            // 这个是什么技巧：先同步，后不控制?
            @Override
            public ZooKeeper getZooKeeper() throws Exception {
                // 延迟创建
                synchronized (this) {
                    if (zooKeeperHandle == null) {
                        connectionString = ensembleProvider.getConnectionString(); // 废话

                        // 饶了一大圈，还是简单地创建一个zk
                        // 创建过程中，可能出现连接错误，例如: 其中的一个zk挂了?
                        zooKeeperHandle = zookeeperFactory.newZooKeeper(connectionString, sessionTimeout, watcher, canBeReadOnly);
                    }

                    helper = new Helper() {
                        @Override
                        public ZooKeeper getZooKeeper() throws Exception {
                            return zooKeeperHandle;
                        }

                        @Override
                        public String getConnectionString() {
                            return connectionString;
                        }
                    };

                    return zooKeeperHandle;
                }
            }

            @Override
            public String getConnectionString() {
                return connectionString;
            }
        };
    }

    private void internalClose() throws Exception {
        try {
            ZooKeeper zooKeeper = (helper != null) ? helper.getZooKeeper() : null;
            if (zooKeeper != null) {
                // 通过空的Watcher, 来clear已有的Watcher
                Watcher dummyWatcher = new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                    }
                };
                zooKeeper.register(dummyWatcher);   // clear the default watcher so that no new events get processed by mistake
                zooKeeper.close();
            }
        } catch (InterruptedException dummy) {
            Thread.currentThread().interrupt();
        }
    }
}
