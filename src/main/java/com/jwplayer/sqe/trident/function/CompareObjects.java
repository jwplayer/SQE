package com.jwplayer.sqe.trident.function;

import com.jwplayer.sqe.language.expression.transform.predicate.NumberComparisonType;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class CompareObjects extends BaseFunction {
    private CompareNumbers numberComparator = new CompareNumbers(NumberComparisonType.Equal);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Object value1 = tuple.get(0);
        Object value2 = tuple.get(1);

        if (value1 instanceof Number && value2 instanceof Number) {
            numberComparator.execute(tuple, collector);
        } else {
            if(value1 == null) collector.emit(new Values(value2 == null));
            else collector.emit(new Values(value1.equals(value2)));
        }
    }
}
