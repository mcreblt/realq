package io.realq.builder.trident.operation;

import io.realq.parser.expr.Expr;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class Add extends BaseFunction {

    private static final long serialVersionUID = -923974492991797974L;
    private List<Expr> arguments;

    public Add(List<Expr> arguments) {
        super();
        this.arguments = arguments;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(this.arguments.get(0).getPrimitiveValue()));
    }
}