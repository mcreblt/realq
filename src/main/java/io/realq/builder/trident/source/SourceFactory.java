package io.realq.builder.trident.source;

import java.util.Properties;

import storm.trident.Stream;
import storm.trident.TridentTopology;


public class SourceFactory {

    public static Stream getStream(String streamId, TridentTopology topology, String spoutClass,
            Properties properties) {

        Stream stream = null;

        switch (spoutClass) {
        case "io.realq.builder.trident.source.FixedBatchSource":
            stream = new FixedBatchSource().getSource(streamId, topology, properties);
            break;
        case "io.realq.builder.trident.source.KafkaSource":
            stream = new KafkaSource().getSource(streamId, topology, properties);
            break;
        default:
            stream = null;
            break;
        }
        return stream;
    }
}
