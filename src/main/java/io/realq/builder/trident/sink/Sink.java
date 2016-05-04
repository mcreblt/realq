package io.realq.builder.trident.sink;

import io.realq.parser.expr.Expr;

import java.util.List;
import java.util.Properties;

import storm.trident.Stream;

public interface Sink {
    public Stream getSink(Stream stream, List<Expr> inputs, Properties properties);
}
