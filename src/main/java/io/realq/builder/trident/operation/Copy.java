package io.realq.builder.trident.operation;

import io.realq.parser.expr.Expr;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class Copy extends BaseFunction {

    private static final long serialVersionUID = 5858133236192650287L;

    public Copy(List<Expr> arguments) {
        super();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        collector.emit(new Values(tuple.toArray()));
    }
}