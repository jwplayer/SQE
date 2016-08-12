package com.jwplayer.sqe.language.state.mongodb;

import com.jwplayer.sqe.language.state.StateAdapter;
import com.jwplayer.sqe.language.state.StateOperationType;
import com.jwplayer.sqe.trident.state.mongodb.MongoDBState;
import com.jwplayer.sqe.trident.state.mongodb.MongoDBStateOptions;

import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;

import java.util.List;

public class MongoDBStateAdapter extends StateAdapter {
    private MongoDBStateOptions options;

    public MongoDBStateAdapter(MongoDBStateOptions options) { this.options = options; }

    @Override
    public StateFactory makeFactory(String objectName, List<String> keyFields, String valueField, StateType stateType, StateOperationType stateOperationType) {
        switch(stateType) {
            case NON_TRANSACTIONAL:
                return MongoDBState.nonTransactional(objectName, keyFields, valueField, options);
            case OPAQUE:
                return MongoDBState.opaque(objectName, keyFields, valueField, options);
            case TRANSACTIONAL:
                return MongoDBState.transactional(objectName, keyFields, valueField, options);
            default:
                throw new RuntimeException("Unknown state type: " + stateType);
        }
    }
}
