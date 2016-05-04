package io.realq.builder.trident.sink;

import io.realq.builder.trident.TridentUtils;
import io.realq.parser.expr.Expr;

import java.util.List;
import java.util.Properties;

import storm.trident.Stream;
import storm.trident.operation.builtin.Debug;
import backtype.storm.tuple.Fields;

public class ConsoleSink implements Sink {

    @Override
    public Stream getSink(Stream stream, List<Expr> inputs, Properties properties) {

        List<String> inputFields = TridentUtils.getFields(inputs);

        stream.each(new Fields(inputFields), new JsonSerializerKeyValue(inputs), new Fields("key", "value"))
              .each(new Fields("key", "value"), new Debug("ConsoleSink"));

        return stream;
    }

}
