package com.jwplayer.sqe.language.state.redis;

import java.util.List;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.trident.state.RedisState;

/**
 * Similar to RedisState.Factory but also allows storing an objectName,
 * used to name-space hash data in Redis
 */
public class RedisStateFactory extends RedisState.Factory{
    private static final long serialVersionUID = 1L;
    public List<String> streamFields;
    public String objectName;


    /**
     * Store objectName for persisting state to Hash (additional key)
     * @param config
     * @param objectName
     */
    public RedisStateFactory(JedisPoolConfig config, String objectName, List<String> streamFields) {
        super(config);
        this.objectName = objectName;
        this.streamFields = streamFields;
    }

}
