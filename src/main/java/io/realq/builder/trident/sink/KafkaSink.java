package io.realq.builder.trident.sink;

import io.realq.builder.trident.TridentUtils;
import io.realq.parser.expr.Expr;

import java.util.List;
import java.util.Properties;

import storm.kafka.trident.TridentKafkaStateFactory;
import storm.kafka.trident.TridentKafkaUpdater;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.trident.Stream;

import backtype.storm.tuple.Fields;

public class KafkaSink implements Sink {
    public Stream getSink(Stream stream, List<Expr> inputs, Properties properties) {

        Properties propsBroker = new Properties();
        
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        
        propsBroker.put("kafka.broker.properties", properties);
        String topic = properties.getProperty("topic");
        List<String> inputFields = TridentUtils.getFields(inputs);

        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactoryWithProps()
                .withProducerProperties(propsBroker)
                .withKafkaTopicSelector(new DefaultTopicSelector(topic))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "value"));

        stream.each(new Fields(inputFields), new JsonSerializerKeyValue(inputs), new Fields("key", "value"))
              .partitionPersist(stateFactory, new Fields("key", "value"), new TridentKafkaUpdater(), new Fields());

        return stream;
    }
}
