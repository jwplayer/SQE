package com.jwplayer.sqe.trident.state.mongodb;

import com.clearspring.analytics.hash.MurmurHash;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.*;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayOutputStream;
import java.util.*;


import static com.mongodb.client.model.Filters.*;

public class MongoDBState<T> implements IBackingMap<T> {
    private MongoClient mongoClient = null;
    private MongoCollection mongoCollection = null;
    private MongoDatabase mongoDB = null;
    private String collectionName;
    private List<String> keyFields;
    private String valueField;
    private MongoDBStateOptions options;
    private StateType stateType;

    public MongoDBState(String collectionName, List<String> keyFields, String valueField, MongoDBStateOptions options, StateType stateType) {
        this.collectionName = collectionName;
        this.keyFields = keyFields;
        this.valueField = valueField;
        this.options = options;
        this.stateType = stateType;
    }

    public static StateFactory nonTransactional(String collectionName, List<String> keyFields, String valueField, MongoDBStateOptions options) {
        return new Factory(collectionName, keyFields, valueField, options, StateType.NON_TRANSACTIONAL);
    }

    public static StateFactory opaque(String collectionName, List<String> keyFields, String valueField, MongoDBStateOptions options) {
        return new Factory(collectionName, keyFields, valueField, options, StateType.OPAQUE);
    }

    public static StateFactory transactional(String collectionName, List<String> keyFields, String valueField, MongoDBStateOptions options) {
        return new Factory(collectionName, keyFields, valueField, options, StateType.TRANSACTIONAL);
    }

    protected static class Factory implements StateFactory {
        private String collectionName;
        private List<String> keyFields;
        private MongoDBStateOptions options;
        private StateType stateType;
        private String valueField;

        public Factory(String collectionName, List<String> keyFields, String valueField, MongoDBStateOptions options, StateType stateType) {
            this.collectionName = collectionName;
            this.keyFields = keyFields;
            this.options = options;
            this.stateType = stateType;
            this.valueField = valueField;
        }

        @Override
        @SuppressWarnings("unchecked")
        public State makeState(Map map, IMetricsContext context, int partitionIndex, int numPartitions) {
            MongoDBState mongoDBState = new MongoDBState(collectionName, keyFields, valueField, options, stateType);
            CachedMap cachedMap = new CachedMap(mongoDBState, options.cacheSize);
            MapState mapState;

            switch(stateType) {
                case NON_TRANSACTIONAL:
                    mapState = NonTransactionalMap.build(cachedMap);
                    break;
                case TRANSACTIONAL:
                    mapState = TransactionalMap.build(cachedMap);
                    break;
                case OPAQUE:
                    mapState = OpaqueMap.build(cachedMap);
                    break;
                default:
                    throw new RuntimeException("Unknown state type: " + stateType);
            }

            return new SnapshottableMap(mapState, new Values(options.globalKey));
        }
    }

    @SuppressWarnings("unchecked")
    private T deserializeValue(Document value) {
        if(value == null) return null;

        // We have to handle the BSON binary value and convert it to byte[]
        Object internalValue = value.get(options.valueName);
        if(internalValue instanceof org.bson.types.Binary) internalValue = ((org.bson.types.Binary) internalValue).getData();

        switch (stateType) {
            case NON_TRANSACTIONAL:
                return (T) internalValue;
            case TRANSACTIONAL:
                return (T) new TransactionalValue<>(value.getLong(options.txIdName), internalValue);
            case OPAQUE:
                Object prevInternalValue = value.get(options.prevValueName);
                if(prevInternalValue instanceof org.bson.types.Binary) prevInternalValue = ((org.bson.types.Binary) prevInternalValue).getData();

                return (T) new OpaqueValue<>(value.getLong(options.txIdName), internalValue, prevInternalValue);
            default:
                return null;
        }
    }

    private Long getID(List<Object> keys) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try {
            for (Object key : keys) {
                stream.write(String.valueOf(key).getBytes());
            }
            stream.write(valueField.getBytes());
        } catch (Exception ex) {
            throw new RuntimeException("Could not create _id from given keys", ex);
        }

        byte[] bytes = stream.toByteArray();
        return MurmurHash.hash64(bytes, bytes.length);
    }

    private MongoClient getMongoClient() {
        if (mongoClient == null) {
            MongoClientOptions clientOptions = MongoClientOptions.builder()
                    .readPreference(ReadPreference.primaryPreferred())
                    .requiredReplicaSetName(options.replicaSet)
                    .build();
            List<ServerAddress> servers = new ArrayList<>();
            for(String host: options.hosts) {
                servers.add(new ServerAddress(host, options.port));
            }
            mongoClient = new MongoClient(
                    servers,
                    Collections.singletonList(MongoCredential.createScramSha1Credential(
                            options.userName,
                            options.db,
                            options.password.toCharArray())),
                    clientOptions);
        }

        return mongoClient;
    }

    @SuppressWarnings("unchecked")
    private MongoCollection<Document> getMongoCollection() {
        if (mongoCollection == null) {
            mongoCollection = getMongoDatabase().getCollection(collectionName);
        }

        return mongoCollection;
    }

    private MongoDatabase getMongoDatabase() {
        if (mongoDB == null) {
            mongoDB = getMongoClient().getDatabase(options.db);
        }

        return mongoDB;
    }

    private Document serializeValue(T value) {
        switch (stateType) {
            case NON_TRANSACTIONAL:
                return new Document(options.valueName, value);
            case TRANSACTIONAL:
                Map<String, Object> tMap = new HashMap<>();
                tMap.put(options.txIdName, ((TransactionalValue) value).getTxid());
                tMap.put(options.valueName, ((TransactionalValue) value).getVal());
                return new Document(tMap);
            case OPAQUE:
                Map<String, Object> oMap = new HashMap<>();
                oMap.put(options.txIdName, ((OpaqueValue) value).getCurrTxid());
                oMap.put(options.valueName, ((OpaqueValue) value).getCurr());
                oMap.put(options.prevValueName, ((OpaqueValue) value).getPrev());
                return new Document(oMap);
            default:
                return null;
        }
    }

    @Override
    public List<T> multiGet(List<List<Object>> keysList) {
        MongoCollection collection = getMongoCollection();
        List<T> retVal = new ArrayList<>();

        for(List<Object> keys: keysList) {
            Document document = (Document) collection.find(eq("_id", getID(keys))).first();
            if(document == null) {
                retVal.add(null);
            }
            else {
                retVal.add(deserializeValue((Document) document.get(valueField)));
            }
        }

        return retVal;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void multiPut(List<List<Object>> keysList, List<T> values) {
        MongoCollection collection = getMongoCollection();
        List<UpdateOneModel> updateOneModels = new ArrayList<>();
        UpdateOptions updateOptions = new UpdateOptions().upsert(true);

        for(int i = 0; i < keysList.size(); i++) {
            Map<String, Object> map = new HashMap<>();
            for(int j = 0; j < keysList.get(i).size(); j++) {
                map.put(keyFields.get(j), keysList.get(i).get(j));
            }
            map.put(valueField, serializeValue(values.get(i)));
            updateOneModels.add(new UpdateOneModel(
                    eq("_id", getID(keysList.get(i))),
                    new Document("$set", new Document(map)),
                    updateOptions));
        }

        collection.bulkWrite(updateOneModels);
    }
}
