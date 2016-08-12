package com.jwplayer.sqe.trident;

import org.apache.storm.trident.operation.TridentCollector;

import java.util.ArrayList;
import java.util.List;


public class ListValuesCollector implements TridentCollector {
    public List<List<Object>> values = new ArrayList<>();

    public void emit(List<Object> values) {
        this.values.add(values);
    }

    public void reportError(Throwable throwable) {
        throw new RuntimeException(throwable);
    }
}
