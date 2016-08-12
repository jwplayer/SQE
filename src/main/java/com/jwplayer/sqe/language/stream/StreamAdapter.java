package com.jwplayer.sqe.language.stream;

import com.clearspring.analytics.hash.MurmurHash;
import com.jwplayer.sqe.language.stream.kafka.KafkaStreamAdapter;
import com.jwplayer.sqe.language.stream.kafka.KafkaStreamAdapterOptions;
import com.jwplayer.sqe.language.stream.testing.FixedBatchSpoutOptions;
import com.jwplayer.sqe.language.stream.testing.FixedBatchSpoutStreamAdapter;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateType;

import java.util.Map;


public abstract class StreamAdapter {
    public abstract Stream makeStream(TridentTopology topology, String topologyName, String streamName, String objectName, StateType spoutType);

    public static final String STREAM_METADATA_FIELD = "_stream_metadata";

    public static Long createPid(String topologyName, String streamName, String secondaryID) {
        return MurmurHash.hash64(topologyName + streamName + secondaryID);
    }

    public static StreamAdapter makeAdapter(String spoutName, Map options) {
        switch(spoutName.toLowerCase()) {
            case "fixed":
                FixedBatchSpoutOptions fixedOptions = FixedBatchSpoutOptions.parse(options);
                return new FixedBatchSpoutStreamAdapter(fixedOptions);
            case "kafka":
                KafkaStreamAdapterOptions kafkaOptions = KafkaStreamAdapterOptions.parse(options);
                return new KafkaStreamAdapter(kafkaOptions);
            default:
                throw new RuntimeException("Unknown spout name: " + spoutName);
        }
    }
}
