package io.realq.builder.trident.operation;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Operator;
import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types;

import java.util.List;

import backtype.storm.tuple.Values;

import com.google.common.primitives.Ints;

public class Comparison extends BaseFunction {

    private static final long serialVersionUID = 2222030299899019417L;
    private List<Expr> args;

    public Comparison(List<Expr> args) {
        super();
        this.args = args;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Expr condition1 = args.get(0);
        Expr operator = args.get(1);
        Expr condition2 = args.get(2);
        Primitive cond1Value = parseExpr(condition1, tuple);
        Operator.Type operatorValue = Operator.Type.valueOf(operator.getStringValue());
        Primitive cond2Value = parseExpr(condition2, tuple);

        Values values = new Values();

        values.add(compare(cond1Value, operatorValue, cond2Value));
        collector.emit(values);
    }

    private Primitive parseExpr(Expr expr, TridentTuple tuple) {
        Primitive primitive = null;
        if (expr.category.equals(Expr.Category.FIELD)) {
            primitive = (Primitive) tuple.getValueByField(expr.getStringValue());
        } else if (expr.category.equals(Expr.Category.LITERAL)) {
            // TODO: is this happening
            primitive = expr.getPrimitiveValue();
        }
        return primitive;
    }

    private Primitive compare(Primitive condition1, Operator.Type operator, Primitive condition2) {
        boolean result = false;
        int[] local = operator.getValue();
        if (Ints.asList(local).contains(condition1.compareTo(condition2))) {
            result = true;
        }

        return new Primitive(result, Types.Type.BOOLEAN);
    }

}
