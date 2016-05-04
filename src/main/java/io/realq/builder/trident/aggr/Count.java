package io.realq.builder.trident.aggr;

import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class Count implements CombinerAggregator<Primitive> {

    private static final long serialVersionUID = -104100602763203122L;

    @Override
    public Primitive init(TridentTuple tuple) {
        return new Primitive(1L, Types.Type.LONG);
    }

    @Override
    public Primitive combine(Primitive val1, Primitive val2) {
        return new Primitive(val1.getLong() + val2.getLong(), Types.Type.LONG);
    }

    @Override
    public Primitive zero() {
        return new Primitive(0L, Types.Type.LONG);
    }

}
