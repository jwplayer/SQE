package com.jwplayer.sqe.trident.function;

import com.jwplayer.sqe.language.expression.transform.predicate.LogicalOperatorType;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class ProcessLogicalOperator extends BaseFunction {
    private LogicalOperatorType operatorType = null;

    public ProcessLogicalOperator(LogicalOperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        Boolean retVal = tuple.getBoolean(0);

        switch(operatorType) {
            case And:
                for(int i = 1; i < tuple.size(); i++) {
                    retVal = retVal && tuple.getBoolean(i);
                }
                break;
            case Not:
                retVal = !retVal;
                break;
            case Or:
                for(int i = 1; i < tuple.size(); i++) {
                    retVal = retVal || tuple.getBoolean(i);
                }
                break;
            case Xor:
                for(int i = 1; i < tuple.size(); i++) {
                    retVal = retVal ^ tuple.getBoolean(i);
                }
                break;
            default:
                throw new UnsupportedOperationException(operatorType.toString() + " is not supported");
        }

        collector.emit(new Values(retVal));
    }
}
