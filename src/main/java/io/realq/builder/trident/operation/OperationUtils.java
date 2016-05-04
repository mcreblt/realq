package io.realq.builder.trident.operation;

import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Primitive;
import storm.trident.tuple.TridentTuple;

public class OperationUtils {
    public static Primitive parseExpr(Expr expr, TridentTuple tuple) {
        Primitive primitive = null;
        if (expr.category.equals(Expr.Category.FIELD)) {
            primitive = (Primitive) tuple.getValueByField(expr.getStringValue());
        } else if (expr.category.equals(Expr.Category.LITERAL)) {
            primitive = expr.getPrimitiveValue();
        }
        return primitive;
    }
}
