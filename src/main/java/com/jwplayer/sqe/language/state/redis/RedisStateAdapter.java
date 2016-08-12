package com.jwplayer.sqe.language.state.redis;

import com.jwplayer.sqe.trident.state.GsonOpaqueSerializer;
import com.jwplayer.sqe.trident.state.GsonTransactionalSerializer;
import com.jwplayer.sqe.language.state.StateAdapter;
import com.jwplayer.sqe.language.state.StateOperationType;
import com.jwplayer.sqe.trident.state.redis.RedisKeyFactory;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.trident.state.Options;
import org.apache.storm.redis.trident.state.RedisDataTypeDescription;
import org.apache.storm.redis.trident.state.RedisMapState;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

import java.util.ArrayList;
import java.util.List;


public class RedisStateAdapter extends StateAdapter {
    protected RedisStateOptions options;

    public RedisStateAdapter(RedisStateOptions options) { this.options = options; }

    @Override
    public TridentState partitionPersist(Stream stream, StateFactory stateFactory, Fields keyFields) {
        RedisStateFactory factory = (RedisStateFactory) stateFactory;
        RedisStateUpdater updater = new RedisStateUpdater(new StoreMapper(this.options,
                                                          factory.objectName, factory.streamFields));
        updater.setExpireInterval(options.expireintervalsec);
        return stream.partitionPersist(stateFactory, keyFields, updater);
    }

    private static class StoreMapper implements RedisStoreMapper {
        private static final long serialVersionUID = 1L;
        private RedisKeyFactory keyStringFactory;
        private RedisKeyFactory valueStringFactory;
        private org.apache.storm.redis.common.mapper.RedisDataTypeDescription description;

        public StoreMapper(RedisStateOptions options, String objectName, List<String> streamFields) {
            // No additional key since not aggregating
            description = new org.apache.storm.redis.common.mapper.RedisDataTypeDescription(
                    org.apache.storm.redis.common.mapper.RedisDataTypeDescription.RedisDataType.SET, 
                    objectName);

            List<Integer> keyNameFieldIndexes = new ArrayList<>();
            List<Integer> fieldNameFieldIndexes = new ArrayList<>();

            for(int i = 0; i < streamFields.size(); i++) {
                if(options.keyNameFields.contains(streamFields.get(i))) keyNameFieldIndexes.add(i);
                if(options.fieldNameFields.contains(streamFields.get(i))) fieldNameFieldIndexes.add(i);
            }
            keyStringFactory = new RedisKeyFactory(options.delimiter, keyNameFieldIndexes, "", "");
            valueStringFactory = new RedisKeyFactory(options.delimiter, fieldNameFieldIndexes, "", "");
        }

        @Override
        public org.apache.storm.redis.common.mapper.RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return keyStringFactory.build(tuple.getValues());
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return valueStringFactory.build(tuple.getValues());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public StateFactory makeFactory(String objectName, List<String> keyFields, String valueField, StateType stateType, StateOperationType stateOperationType) {
        JedisPoolConfig poolConfig =
                new JedisPoolConfig.Builder()
                        .setDatabase(options.database)
                        .setHost(options.host)
                        .setPort(options.port)
                        .setTimeout(15000)
                        .build();
        Options rOptions = new Options();

        // Build the Redis options based on configuration from SQE
        switch(options.dataType) {
            case STRING:
                List<Integer> indexes = new ArrayList<>();

                for(int i = 0; i < keyFields.size(); i++) indexes.add(i);

                rOptions.keyFactory = new RedisKeyFactory(options.delimiter, indexes, objectName, valueField);
                rOptions.dataTypeDescription = new RedisDataTypeDescription(options.dataType);
                break;
            case HASH:
            case SET:
                List<Integer> keyNameFieldIndexes = new ArrayList<>();
                List<Integer> fieldNameFieldIndexes = new ArrayList<>();

                for(int i = 0; i < keyFields.size(); i++) {
                    if(options.keyNameFields.contains(keyFields.get(i))) keyNameFieldIndexes.add(i);
                    if(options.fieldNameFields.contains(keyFields.get(i))) fieldNameFieldIndexes.add(i);
                }

                rOptions.keyFactory = new RedisKeyFactory(options.delimiter, keyNameFieldIndexes, objectName, "");
                rOptions.dataTypeDescription =
                        new RedisDataTypeDescription(options.dataType,
                                new RedisKeyFactory(options.delimiter, fieldNameFieldIndexes, "", valueField));
                break;
            case LIST:
            case SORTED_SET:
            case HYPER_LOG_LOG:
                throw new IllegalArgumentException("Unsupported Redis data type: " + options.dataType);
        }

        // Set TTL for the keys
        rOptions.expireIntervalSec = options.expireintervalsec;

        if (stateOperationType == StateOperationType.NONAGGREGATE){
            return new RedisStateFactory(poolConfig, objectName, keyFields);
        }
        // Return the appropriate state factory
        switch(stateType) {
            case NON_TRANSACTIONAL:
                return RedisMapState.nonTransactional(poolConfig, rOptions);
            case OPAQUE:
                rOptions.serializer = new GsonOpaqueSerializer();
                return RedisMapState.opaque(poolConfig, rOptions);
            case TRANSACTIONAL:
                rOptions.serializer = new GsonTransactionalSerializer();
                return RedisMapState.transactional(poolConfig, rOptions);
            default:
                throw new RuntimeException("Unknown state type: " + stateType);
        }
    }
}
