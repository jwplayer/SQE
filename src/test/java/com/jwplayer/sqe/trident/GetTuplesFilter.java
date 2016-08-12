package com.jwplayer.sqe.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class GetTuplesFilter extends BaseFilter {
    public List<TridentTuple> tuples;

    public GetTuplesFilter() {
        this.tuples = new ArrayList<>();
    }

    public boolean isKeep(TridentTuple tuple) {
        System.out.println("TUPLES: " + tuple.toString());
        tuples.add(tuple);
        System.out.println("TUPLES: " + tuples.size());

        return true;
    }
}
