package io.realq.builder.trident.operation;

import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types;
import io.realq.parser.expr.Expr.Category;

import java.util.List;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class Filter extends BaseFilter {

    private static final long serialVersionUID = -548917318287478330L;
    private List<Expr> exprs;

    public Filter(List<Expr> exprs) {
        super();
        this.exprs = exprs;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        boolean result = false;
        if (exprs.get(0).category.equals(Category.FIELD)) {
            Primitive primitive = (Primitive) tuple.getValueByField(exprs.get(0).value.toString());
            if(primitive.getType() == Types.Type.BOOLEAN){
                result = primitive.getBoolean();
            } 
        }

        return result;
    }
}