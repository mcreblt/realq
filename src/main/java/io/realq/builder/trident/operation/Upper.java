package io.realq.builder.trident.operation;

import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class Upper extends BaseFunction {

    private static final long serialVersionUID = -4354904853876872528L;
    private List<Expr> exprs;

    public Upper(List<Expr> exprs) {
        super();
        this.exprs = exprs;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Expr expr = exprs.get(0);
        Primitive primitive = (Primitive) tuple.getValueByField(expr.value.toString());

        Values values = new Values();

        if(primitive.getType() == Types.Type.STRING){
            values.add(new Primitive(primitive.getString().toUpperCase(), Types.Type.STRING));   
        } else {
            values.add(primitive);   
        }
        
        collector.emit(values);
    }
}