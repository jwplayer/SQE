package com.jwplayer.sqe.trident;

import org.apache.storm.trident.operation.TridentCollector;

import java.util.List;


public class SingleValuesCollector implements TridentCollector {
    public List<Object> values = null;

    public void emit(List<Object> values) {
        this.values = values;
    }

    public void reportError(Throwable throwable) {
        throw new RuntimeException(throwable);
    }
}
