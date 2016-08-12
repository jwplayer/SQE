package com.jwplayer.sqe.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Date;


public class RoundDate extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            Date date = (Date) tuple.get(0);
            Integer amount = ((Number) tuple.get(1)).intValue();
            String unit = tuple.getString(2);
            Long time = date.getTime();
            Long adjustment;

            switch(unit.toLowerCase()) {
                case "second":
                    adjustment = 1000l * amount;
                    break;
                case "minute":
                    adjustment = 60000l * amount;
                    break;
                case "hour":
                    adjustment = 3600000l * amount;
                    break;
                case "day":
                    adjustment = 86400000l * amount;
                    break;
                default:
                    throw new RuntimeException(unit + " is not a valid unit");
            }

            time = time / adjustment * adjustment;
            collector.emit(new Values(new Date(time)));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
