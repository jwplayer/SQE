package com.jwplayer.sqe.language.stream.testing;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class FixedBatchSpoutOptions implements Serializable {
    public Fields fields;
    public List<List<Object>> values;

    @SuppressWarnings("unchecked")
    public static FixedBatchSpoutOptions parse(Map map) {
        FixedBatchSpoutOptions options = new FixedBatchSpoutOptions();

        if(map.containsKey("jw.sqe.spout.fixed.fields")) options.fields =
                new Fields((List) map.get("jw.sqe.spout.fixed.fields"));
        options.values = new LinkedList<>();
        if(map.containsKey("jw.sqe.spout.fixed.values")) {
            List<List<Object>> rawValues = (List) map.get("jw.sqe.spout.fixed.values");

            for(List<Object> rawValue: rawValues) {
                options.values.add(new Values(rawValue.toArray()));
            }
        }

        return options;
    }
}
