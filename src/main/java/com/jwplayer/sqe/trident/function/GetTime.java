package com.jwplayer.sqe.trident.function;

import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


public class GetTime extends BaseFunction {
    private static final Logger LOG = Logger.getLogger(GetTime.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            Date isoDate;

            if(tuple.size() == 2) {
                SimpleDateFormat isoDateFormat = new SimpleDateFormat(tuple.getString(1));
                isoDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                isoDate = isoDateFormat.parse(tuple.getString(0));
            } else {
                isoDate = (Date) tuple.get(0);
            }

            collector.emit(new Values(isoDate.getTime()));
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }
}
