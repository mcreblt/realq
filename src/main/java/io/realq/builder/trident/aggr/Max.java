package io.realq.builder.trident.aggr;

import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types.Type;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class Max implements CombinerAggregator<Primitive> {

    private static final long serialVersionUID = -1420082926060769864L;
    Type type;
    
    @Override
    public Primitive init(TridentTuple tuple) {
        Primitive primitive = (Primitive) tuple.getValue(0);
        type = primitive.getType();
        return (Primitive) tuple.getValue(0);
    }

    @Override
    public Primitive combine(Primitive val1, Primitive val2) {
        return val2.compareTo(val1) == -1 ? val1 : val2;
    }

    @Override
    public Primitive zero() {
        return new Primitive(0, type);
    }

}
