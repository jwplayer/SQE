package com.jwplayer.sqe.trident.state.mongodb;


import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class MongoDBStateOptions implements Serializable {
    public int cacheSize = 5000;
    public String db = "";
    public String globalKey = "$MONGO-DB-STATE-GLOBAL";
    public List<String> hosts = null;
    public String password = "";
    public int port = 27017;
    public String prevValueName = "Prev";
    public String replicaSet = "";
    public String txIdName = "TxId";
    public String userName = "";
    public String valueName = "Value";

    @SuppressWarnings("unchecked")
    public static MongoDBStateOptions parse(Map map) {
        MongoDBStateOptions options = new MongoDBStateOptions();

        if(map.containsKey("jw.sqe.state.mongodb.cachesize")) options.cacheSize = (int) map.get("jw.sqe.state.mongodb.cachesize");
        if(map.containsKey("jw.sqe.state.mongodb.db")) options.db = (String) map.get("jw.sqe.state.mongodb.db");
        if(map.containsKey("jw.sqe.state.mongodb.hosts")) options.hosts = (List<String>) map.get("jw.sqe.state.mongodb.hosts");
        if(map.containsKey("jw.sqe.state.mongodb.password")) options.password = (String) map.get("jw.sqe.state.mongodb.password");
        if(map.containsKey("jw.sqe.state.mongodb.port")) options.port = Integer.parseInt((String) map.get("jw.sqe.state.mongodb.port"));
        if(map.containsKey("jw.sqe.state.mongodb.replicaSet")) options.replicaSet = (String) map.get("jw.sqe.state.mongodb.replicaSet");
        if(map.containsKey("jw.sqe.state.mongodb.userName")) options.userName = (String) map.get("jw.sqe.state.mongodb.userName");

        return options;
    }
}
