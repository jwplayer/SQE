package com.jwplayer.sqe.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


public class EvaluateRegularExpression extends BaseFunction {
    Map<String, Pattern> patternCache = new HashMap<>();

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Pattern pattern;
        String regex = tuple.getString(1);

        if(patternCache.containsKey(regex)) {
            pattern = patternCache.get(regex);
        } else {
            pattern = Pattern.compile(regex);
            patternCache.put(regex, pattern);
        }

        collector.emit(new Values(pattern.matcher(tuple.getString(0)).matches()));
    }
}
