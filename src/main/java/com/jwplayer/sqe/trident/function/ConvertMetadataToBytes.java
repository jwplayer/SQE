package com.jwplayer.sqe.trident.function;

import com.jwplayer.sqe.trident.StreamMetadata;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class ConvertMetadataToBytes extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        StreamMetadata metadata = (StreamMetadata) tuple.get(0);

        collector.emit(new Values(metadata.toBytes()));
    }
}
