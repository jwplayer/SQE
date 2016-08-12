package com.jwplayer.sqe;

import static org.junit.Assert.*;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.jwplayer.sqe.trident.state.GsonTransactionalSerializer;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.apache.storm.trident.state.TransactionalValue;

import java.util.concurrent.TimeUnit;


public class IntegrationTest {
    String hostName;

    @Before
    public void setup() {
        hostName = "..."; //TODO: This should be a test instance
    }

    public JedisPool getJedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();

        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setMaxTotal(1);
        poolConfig.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(60));

        return new JedisPool(poolConfig, hostName, 6379, 15000, null, 3);
    }

    @SuppressWarnings("unchecked")
    //@Test
    // This test is disabled until we can have a better way to setup test assets so we don't have collisions
    public void testComplexQuery() throws Exception {
        // Delete Redis keys
        JedisPool pool = getJedisPool();
        Jedis jedis = pool.getResource();
        jedis.hdel("TestComplexQuery:2015-05-01", "Account1:UserHllBitmap");
        jedis.hdel("TestComplexQuery:2015-05-01", "Account1:HappyDanceCount");
        jedis.hdel("TestComplexQuery:2015-05-01", "Account2:UserHllBitmap");
        jedis.hdel("TestComplexQuery:2015-05-01", "Account2:HappyDanceCount");

        // Run test topologies
        String[] args = new String[9];

        args[0] = "--commands";
        args[1] = "resource:///queries/test-complex-query.json";
        args[2] = "--config";
        args[3] = "./conf/sample-conf.yaml";
        args[4] = "--local";
        args[5] = "--name";
        args[6] = "TestTopo";
        args[7] = "--runtime";
        args[8] = "120";

        Topology.main(args);

        // Test keys from Redis
        GsonTransactionalSerializer serde = new GsonTransactionalSerializer();
        TransactionalValue<byte[]> bitmapValue = serde.deserialize(jedis.hget("TestComplexQuery:2015-05-01", "Account1:UserHllBitmap").getBytes());
        assertEquals(HyperLogLog.Builder.build(bitmapValue.getVal()).cardinality(), 3l);
        TransactionalValue<Double> doubleValue = serde.deserialize(jedis.hget("TestComplexQuery:2015-05-01", "Account2:HappyDanceCount").getBytes());
        assertEquals(doubleValue.getVal(), 4.0d, 0.0d); // GSON converts numbers to double by default
        pool.returnResource(jedis);
    }
}