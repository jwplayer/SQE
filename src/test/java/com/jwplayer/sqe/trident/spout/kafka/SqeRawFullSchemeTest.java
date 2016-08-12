package com.jwplayer.sqe.trident.spout.kafka;


import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.Partition;
import org.apache.storm.kafka.ZkHosts;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SqeRawFullSchemeTest {
    private SqeRawFullScheme scheme;

    @Before
    public void setup() {
        scheme = new SqeRawFullScheme("test-topo", "test-stream", new ZkHosts("brokers", "path"));
    }

    @Test
    public void nullKeyTest() {
        List<Object> values = scheme.deserialize(null, null, new Partition(new Broker("host"), "topic", 1), 0);
        Assert.assertEquals(values.get(0), null);
        Assert.assertEquals(values.get(1), null);
    }
}
