package com.jwplayer.sqe.language.state.kafka;

import java.util.ArrayList;
import java.util.List;


import com.jwplayer.sqe.language.state.StateAdapter;
import com.jwplayer.sqe.language.state.StateOperationType;
import com.jwplayer.sqe.language.stream.StreamAdapter;
import com.jwplayer.sqe.trident.function.ConvertMetadataToBytes;
import com.jwplayer.sqe.trident.function.Hash;
import org.apache.storm.kafka.trident.TridentKafkaUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.tuple.Fields;


public class KafkaStateAdapter extends StateAdapter {
    public static final String KAFKA_KEY = "_kafka_state_key";
    protected KafkaStateOptions options ;

    public KafkaStateAdapter(KafkaStateOptions options) {
        this.options = options;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StateFactory makeFactory(String objectName, List<String> keyFields, String valueField, StateType stateType, StateOperationType stateOperationType) {
        return getTridentStateFactory(objectName);
    }

    protected JwTridentKafkaStateFactory getTridentStateFactory(String topic) {
        JwTridentKafkaStateFactory factory = new JwTridentKafkaStateFactory();
        factory.withKafkaOptions(options).withKafkaTopicSelector(new DefaultTopicSelector(topic));

        return factory;
    }

    public TridentState partitionPersist(Stream stream, StateFactory stateFactory, Fields keyFields) {
        JwTridentKafkaStateFactory factory = (JwTridentKafkaStateFactory) stateFactory;
        List<String> persistFields = new ArrayList<>();
        for(String fieldName: keyFields) persistFields.add(fieldName);

        if(options.KeyType.equals("field")) {
            factory = factory.withTridentTupleToKafkaMapper(
                    new FieldNameBasedTupleToKafkaMapper<byte[], byte[]>(keyFields.get(0), keyFields.get(1)));
        } else {
            factory = factory.withTridentTupleToKafkaMapper(
                    new FieldNameBasedTupleToKafkaMapper<byte[], byte[]>(KafkaStateAdapter.KAFKA_KEY, keyFields.get(0)));
            persistFields.add(KafkaStateAdapter.KAFKA_KEY);
        }

        switch (options.KeyType) {
            case "field":
                break;
            case "messagehash":
                stream = stream.each(new Fields(keyFields.get(0)), new Hash(), new Fields(KafkaStateAdapter.KAFKA_KEY));
                break;
            case "streammetadata":
                stream = stream.each(
                        new Fields(StreamAdapter.STREAM_METADATA_FIELD),
                        new ConvertMetadataToBytes(),
                        new Fields(KafkaStateAdapter.KAFKA_KEY)
                );
                break;
            default:
                throw new IllegalArgumentException(options.KeyType + " is not a supported key type");
        }

        return stream.partitionPersist(factory, new Fields(persistFields), new TridentKafkaUpdater());
    }
}