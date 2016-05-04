package io.realq.builder.trident.operation;

import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Expr.Category;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class DebugPrimitive extends BaseFilter {

    private static final long serialVersionUID = 3460645553265904720L;
    private String name;
    private Fields fields;

    private List<Expr> exprs;

    public DebugPrimitive(List<Expr> exprs) {
        super();
        this.exprs = exprs;
    }

    public DebugPrimitive(Fields fields) {
        name = "DEBUG: ";
        this.fields = fields;
    }

    public DebugPrimitive(String name, Fields fields) {
        this.name = "DEBUG(" + name + "): ";
        this.fields = fields;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        List<String> output = new ArrayList<String>();
        for (Expr expr : exprs) {
            if (expr.category.equals(Category.FIELD)) {
                Primitive primitive = (Primitive) tuple.getValueByField(expr.value.toString());
                output.add(expr.value + " - " + primitive.getValue().getClass().getSimpleName() + ": "
                        + primitive.getValue());
            }
        }
        System.out.println(output);

        return true;
    }
}