package com.jwplayer.sqe.trident.function;

import static org.junit.Assert.*;

import com.jwplayer.sqe.trident.SingleValuesCollector;
import org.junit.Test;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import java.util.Date;


public class FormatDateTest {
    @Test
    public void testFormatDate() {
        FormatDate formatDate = new FormatDate();
        Date date = new Date(0l);
        String format = "yyyy-MM-dd HH:mm:ss";
        TridentTuple tuple = TridentTupleView.createFreshTuple(new Fields("Date", "Format"), date, format);
        SingleValuesCollector collector = new SingleValuesCollector();

        formatDate.execute(tuple, collector);

        assertEquals(collector.values.get(0), "1970-01-01 00:00:00");
    }
}
