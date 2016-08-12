package com.jwplayer.sqe.trident.function;

import clojure.lang.Numbers;
import com.jwplayer.sqe.language.expression.transform.predicate.NumberComparisonType;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class CompareNumbers extends BaseFunction {
    private NumberComparisonType comparisonType = null;

    public CompareNumbers(NumberComparisonType comparisonType) {
        this.comparisonType = comparisonType;
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        Number value1 = (Number) tuple.get(0);
        Number value2 = (Number) tuple.get(1);

        switch (comparisonType) {
            case Equal:
                collector.emit(new Values(Numbers.equiv(value1, value2)));
                break;
            case GreaterThan:
                collector.emit(new Values(Numbers.gt(value1, value2)));
                break;
            case GreaterThanOrEqual:
                collector.emit(new Values(Numbers.gte(value1, value2)));
                break;
            case LessThan:
                collector.emit(new Values(Numbers.lt(value1, value2)));
                break;
            case LessThanOrEqual:
                collector.emit(new Values(Numbers.lte(value1, value2)));
                break;
            case NotEqual:
                collector.emit(new Values(!Numbers.equiv(value1, value2)));
                break;
            default:
                throw new UnsupportedOperationException(comparisonType.toString() + " is not supported");
        }
    }
}
