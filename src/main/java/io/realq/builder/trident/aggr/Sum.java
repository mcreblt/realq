package io.realq.builder.trident.aggr;

import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Types.Type;


import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class Sum implements CombinerAggregator<Primitive> {

    private static final long serialVersionUID = 7224247029879496045L;
    Type type;
    
    @Override
    public Primitive init(TridentTuple tuple) {
        Primitive primitive = (Primitive) tuple.getValue(0);
        type = primitive.getType();
        return (Primitive) tuple.getValue(0);
    }

    @Override
    public Primitive combine(Primitive o1, Primitive o2) {
        Object result;
        switch (this.type) {
        case NULL:
            result = 0;

        case INTEGER:
            result = o1.getInteger()+o2.getInteger();
            break;
        case LONG:
            result = o1.getLong()+o2.getLong();
            break;
        case FLOAT:
            result = o1.getFloat()+o2.getFloat();
            break;
        case DOUBLE:
            result = o1.getDouble()+o2.getDouble();
            break;
        case BIGDECIMAL:
            result = o1.getBigDecimal().add(o2.getBigDecimal());
            break;
        default:
            result = 0;
        }
        
        return new Primitive(result, this.type);
    }

    @Override
    public Primitive zero() {
        return new Primitive(0, type);
    }

}
