package com.jwplayer.sqe.language.stream.kafka;

import com.google.common.base.Joiner;
import com.jwplayer.sqe.trident.spout.kafka.SqeRawFullScheme;
import org.apache.storm.kafka.FullSchemeAsMultiScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TridentKafkaConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Map;


public class KafkaStreamAdapterOptions implements Serializable {
    Integer bufferSizeBytes = null;
    String clientID;
    Integer fetchSizeBytes = null;
    Boolean filterReplays = false;
    Long filterReplaysMetadataTtl = 60L * 60L * 24L * 2L;
    Long maxOffsetBehind = null;
    ZkHosts zkHosts;

    public TridentKafkaConfig getKafkaConfig(String topologyName, String streamName, String topic) {
        TridentKafkaConfig config = new TridentKafkaConfig(zkHosts, topic, clientID);
        if(bufferSizeBytes != null) config.bufferSizeBytes = bufferSizeBytes;
        if(fetchSizeBytes != null) config.fetchSizeBytes = fetchSizeBytes;
        if(maxOffsetBehind != null) config.maxOffsetBehind = maxOffsetBehind;
        config.scheme = new FullSchemeAsMultiScheme(
                new SqeRawFullScheme(topologyName, streamName, zkHosts)
        );

        return config;
    }

    @SuppressWarnings("unchecked")
    public static KafkaStreamAdapterOptions parse(Map map) {
        KafkaStreamAdapterOptions options = new KafkaStreamAdapterOptions();
        options.zkHosts = new ZkHosts(Joiner.on(',').join((List<String>) map.get("jw.sqe.spout.kafka.zkhosts")));
        options.clientID = (String) map.get("jw.sqe.spout.kafka.clientid");

        if(map.containsKey("jw.sqe.spout.kafka.bufferSizeBytes"))
            options.bufferSizeBytes = (int) map.get("jw.sqe.spout.kafka.bufferSizeBytes");
        if(map.containsKey("jw.sqe.spout.kafka.fetchSizeBytes"))
            options.fetchSizeBytes = (int) map.get("jw.sqe.spout.kafka.fetchSizeBytes");
        if(map.containsKey("jw.sqe.spout.kafka.filterReplays"))
            options.filterReplays = (boolean) map.get("jw.sqe.spout.kafka.filterReplays");
        if(map.containsKey("jw.sqe.spout.kafka.filterReplays.metadata.ttl"))
            options.filterReplaysMetadataTtl = (long) map.get("jw.sqe.spout.kafka.filterReplays.metadata.ttl");
        if(map.containsKey("jw.sqe.spout.kafka.maxOffsetBehind"))
            options.maxOffsetBehind = (long) map.get("jw.sqe.spout.kafka.maxOffsetBehind");

        return options;
    }
}
