package io.realq.builder.trident.operation;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types;

import java.util.List;

import backtype.storm.tuple.Values;

public class If extends BaseFunction {

    private static final long serialVersionUID = -3422493924343598067L;
    private List<Expr> args;

    public If(List<Expr> args) {
        super();
        this.args = args;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Expr condition = args.get(0);

        Primitive conditionValue = parseExpr(condition, tuple);

        Expr result = null;

        if (conditionValue.getType() == Types.Type.BOOLEAN) {
            if (conditionValue.getBoolean()) {
                result = args.get(1);
            } else {
                result = args.get(2);
            }
        } else {
            result = args.get(1);
        }
        
        Values values = new Values();

        values.add(parseExpr(result, tuple));
        collector.emit(values);
    }

    private Primitive parseExpr(Expr expr, TridentTuple tuple) {
        Primitive primitive = null;
        if (expr.category.equals(Expr.Category.FIELD)) {
            primitive = (Primitive) tuple.getValueByField(expr.getStringValue());
        } else if (expr.category.equals(Expr.Category.LITERAL)) {
            primitive = expr.getPrimitiveValue();
        }
        return primitive;
    }

}
