package com.jwplayer.sqe.trident.function;

import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import java.util.Date;


import static org.junit.Assert.assertEquals;

public class GetTimeTest {
    @Test
    public void testGetTimeFromDate() {
        GetTime getTime = new GetTime();
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("Date"), new Date(0l));
        SingleValuesCollector collector = new SingleValuesCollector();

        getTime.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0l);
    }

    @Test
    public void testGetTimeFromString() {
        GetTime getTime = new GetTime();
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("Date", "DateFormat"), "1970-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
        SingleValuesCollector collector = new SingleValuesCollector();

        getTime.execute(tuple, collector);
        assertEquals(collector.values.get(0), 0l);
    }
}
