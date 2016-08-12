package com.jwplayer.sqe.language.state.redis;

import org.apache.storm.redis.trident.state.RedisDataTypeDescription.RedisDataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisStateOptions implements Serializable {
    public int database = 0;
    public RedisDataType dataType = RedisDataType.STRING;
    public String delimiter = ":";
    public List<String> fieldNameFields = new ArrayList<>();
    public String host = "";
    public List<String> keyNameFields = new ArrayList<>();
    public int port = 6379;
    public int expireintervalsec = 0;

    @SuppressWarnings("unchecked")
    public static RedisStateOptions parse(Map map) {
        RedisStateOptions options = new RedisStateOptions();

        if(map.containsKey("jw.sqe.state.redis.database"))
            options.database = ((Number) map.get("jw.sqe.state.redis.database")).intValue();
        if(map.containsKey("jw.sqe.state.redis.datatype"))
            options.dataType = RedisDataType.valueOf((String) map.get("jw.sqe.state.redis.datatype"));
        if(map.containsKey("jw.sqe.state.redis.delimiter"))
            options.delimiter = (String) map.get("jw.sqe.state.redis.delimiter");
        if(map.containsKey("jw.sqe.state.redis.expireintervalsec"))
            options.expireintervalsec = ((Number)map.get("jw.sqe.state.redis.expireintervalsec")).intValue();
        if(map.containsKey("jw.sqe.state.redis.fieldname.fields"))
            options.fieldNameFields = (List<String>) map.get("jw.sqe.state.redis.fieldname.fields");
        if(map.containsKey("jw.sqe.state.redis.host"))
            options.host = (String) map.get("jw.sqe.state.redis.host");
        if(map.containsKey("jw.sqe.state.redis.keyname.fields"))
            options.keyNameFields = (List<String>) map.get("jw.sqe.state.redis.keyname.fields");
        if(map.containsKey("jw.sqe.state.redis.port"))
            options.port = Integer.parseInt((String) map.get("jw.sqe.state.redis.port"));

        return options;
    }
}
