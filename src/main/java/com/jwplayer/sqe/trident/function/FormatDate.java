package com.jwplayer.sqe.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


public class FormatDate extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(tuple.getString(1));
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            String date = dateFormat.format((Date) tuple.get(0));

            collector.emit(new Values(date));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
