package com.jwplayer.sqe.trident.function;

import com.clearspring.analytics.hash.MurmurHash;
import com.google.common.primitives.Longs;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class Hash extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String hashName = "murmur2";
        Object object;

        if(tuple.size() == 1) {
            object = tuple.get(0);
        } else {
            hashName = tuple.getString(0);
            object = tuple.get(1);
        }

        // Emits a hash as byte[]
        switch(hashName.toLowerCase()) {
            // Uses Clearsprings Murmur 2.0 implementation
            case "murmur2":
                Long m2Hash = MurmurHash.hash64(object);
                byte[] m2Bytes = Longs.toByteArray(m2Hash);
                collector.emit(new Values(m2Bytes));
                break;
            default:
                throw new UnsupportedOperationException(hashName + "is not a supported hashing algorithm");
        }
    }
}
