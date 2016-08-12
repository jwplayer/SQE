package com.jwplayer.sqe.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class ValueFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        return tuple.getBoolean(0);
    }
}
