package io.realq.builder.trident.sink;

import io.realq.parser.expr.Expr;

import java.util.List;
import java.util.Properties;

import storm.trident.Stream;

public class SinkFactory {

    public static Stream getStream(Stream stream, String sinkClass, List<Expr> inputs,
            Properties properties) {

        switch (sinkClass) {
        case "io.realq.builder.trident.sink.ConsoleSink":
            stream = new ConsoleSink().getSink(stream, inputs, properties);
            break;
        case "io.realq.builder.trident.sink.KafkaSink":
            stream = new KafkaSink().getSink(stream, inputs, properties);
            break;
        default:
            stream = null;
            break;
        }

        return stream;
    }
}
