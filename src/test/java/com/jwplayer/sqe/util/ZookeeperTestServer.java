package com.jwplayer.sqe.util;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.framework.imps.CuratorFrameworkState;

import java.io.IOException;


public class ZookeeperTestServer {
    private CuratorFramework zookeeperTestServer;
    private TestingServer testingServer;

    public ZookeeperTestServer() {
        try {
            testingServer = new TestingServer();
            ExponentialBackoffRetry backoffRetry = new ExponentialBackoffRetry(1000, 4);
            zookeeperTestServer = CuratorFrameworkFactory.newClient(getConnectionString(), backoffRetry);
        } catch (Exception ex) {
            throw new RuntimeException("Couldn't start test ZK server", ex);
        }
    }

    public String getConnectionString() {
        return testingServer.getConnectString();
    }

    public ZkUtils getZkUtils() {
        ZkClient zkClient = new ZkClient(getConnectionString());
        return new ZkUtils(zkClient, new ZkConnection(getConnectionString()), false);
    }

    public void shutdown() {
        try {
            if (zookeeperTestServer.getState().equals(CuratorFrameworkState.STARTED)) {
                zookeeperTestServer.close();
            }
            testingServer.close();
        } catch (IOException ex) {
            throw new RuntimeException("Couldn't shutdown test ZK server", ex);
        }
    }
}
