package io.realq.builder.trident.test.utils;

import io.realq.parser.expr.Expr;
import io.realq.parser.expr.Primitive;
import io.realq.parser.expr.Expr.Category;

import java.util.List;

import org.apache.commons.lang.math.NumberUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class Split extends BaseFunction {

    private List<Expr> exprs;

    public Split(List<Expr> exprs) {
        super();
        this.exprs = exprs;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Expr expr = exprs.get(0);

        if (expr.category.equals(Category.FIELD)) {

            String output = (String) tuple.getValueByField(expr.value.toString());
            String[] msgs = output.split(";");
            for (String msg : msgs) {
                Values values = new Values();
                String[] parts = msg.split(" ");
                for (String part : parts) {
                    if (NumberUtils.isNumber(part)) {
                        values.add(new Primitive(NumberUtils.createNumber(part)));
                    } else {
                        values.add(new Primitive(part));
                    }

                }
                collector.emit(values);
            }
        }
    }
}