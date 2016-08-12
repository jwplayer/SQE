package com.jwplayer.sqe.trident.function;

import com.google.common.primitives.Longs;
import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import static org.junit.Assert.*;


public class HashTest {
    @Test
    public void testMurmur2Hash() {
        Hash hash = new Hash();
        SingleValuesCollector collector = new SingleValuesCollector();
        TridentTuple tuple =
                TridentTupleView.createFreshTuple(new Fields("HashName", "ValueToHash"), "Murmur2", "ABCDEF");

        hash.execute(tuple, collector);
        assertEquals(Longs.fromByteArray((byte[]) collector.values.get(0)), 0xF6106105304A543l);
    }
}
