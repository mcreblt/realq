package io.realq.builder.trident.operation;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import io.realq.parser.expr.Expr;
import io.realq.parser.expr.OperatorLogical;
import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types;

import java.math.BigDecimal;

import java.util.List;

import backtype.storm.tuple.Values;


public class Logical extends BaseFunction {

    private static final long serialVersionUID = 6471909801935535148L;
    private List<Expr> args;

    public Logical(List<Expr> args) {
        super();
        this.args = args;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Expr condition1 = args.get(0);
        Expr operator = args.get(1);
        Expr condition2 = args.get(2);
        Primitive cond1Value = OperationUtils.parseExpr(condition1, tuple);
        OperatorLogical.Type operatorValue = OperatorLogical.Type.valueOf(operator.getStringValue());
        Primitive cond2Value = OperationUtils.parseExpr(condition2, tuple);

        Values values = new Values();
        if (operatorValue.equals(OperatorLogical.Type.AND)) {
            values.add(new Primitive(cond1Value.toBooleanValue() && cond2Value.toBooleanValue(), Types.Type.BOOLEAN));
        } else if (operatorValue.equals(OperatorLogical.Type.OR)) {
            values.add(new Primitive(cond1Value.toBooleanValue() || cond2Value.toBooleanValue()));
        } else if (operatorValue.equals(OperatorLogical.Type.PLUS)) {
            BigDecimal value1 = new BigDecimal(cond1Value.getString());
            BigDecimal value2 = new BigDecimal(cond2Value.getString());
            values.add(new Primitive(value1.add(value2), Types.Type.BIGDECIMAL));
        }

        collector.emit(values);
    }

}
