package io.realq.builder.trident.operation;

import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class Lower extends BaseFunction {

    private static final long serialVersionUID = -4683245086555366441L;
    private List<Expr> exprs;

    public Lower(List<Expr> exprs) {
        super();
        this.exprs = exprs;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Expr expr = exprs.get(0);
        Primitive primitive = (Primitive) tuple.getValueByField(expr.value.toString());

        Values values = new Values();

        if(primitive.getType() == Types.Type.STRING){
            values.add(new Primitive(primitive.getString().toLowerCase(), Types.Type.STRING));   
        } else {
            values.add(primitive);   
        }
        
        collector.emit(values);
    }
}