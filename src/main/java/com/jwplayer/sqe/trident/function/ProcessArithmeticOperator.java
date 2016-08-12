package com.jwplayer.sqe.trident.function;

import clojure.lang.Numbers;
import clojure.lang.Ratio;
import com.jwplayer.sqe.language.expression.transform.ArithmeticOperatorType;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;


public class ProcessArithmeticOperator extends BaseFunction {
    private ArithmeticOperatorType operatorType = null;

    public ProcessArithmeticOperator(ArithmeticOperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        Number value1 = (Number) tuple.get(0);
        Number value2 = (Number) tuple.get(1);

        switch(operatorType) {
            case Addition:
                collector.emit(new Values(Numbers.add(value1, value2)));
                break;
            case Division:
                Number retVal = Numbers.divide(value1, value2);

                if(retVal instanceof Ratio) collector.emit(new Values(retVal.doubleValue()));
                else collector.emit(new Values(retVal));
                break;
            case Modulus:
                collector.emit(new Values(Numbers.remainder(value1, value2)));
                break;
            case Multiplication:
                collector.emit(new Values(Numbers.multiply(value1, value2)));
                break;
            case Subtraction:
                collector.emit(new Values(Numbers.minus(value1, value2)));
                break;
            default:
                throw new UnsupportedOperationException(operatorType.toString() + " is not supported");
        }
    }
}
