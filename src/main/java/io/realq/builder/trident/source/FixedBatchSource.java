package io.realq.builder.trident.source;

import java.util.Properties;

import org.apache.commons.lang.StringEscapeUtils;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FixedBatchSource implements Source {

    @Override
    public Stream getSource(String streamId, TridentTopology topology, Properties properties) {

        String valueSeperator = properties.getProperty("valuesSeperator");
        String[] stringValues = properties.getProperty("values").split(valueSeperator);
        Values[] values = new Values[stringValues.length];
        for (int i = 0; i < stringValues.length; i++) {
            values[i] = new Values(StringEscapeUtils.unescapeJava(stringValues[i]));
        }

        FixedBatchSpout spout = new FixedBatchSpout(new Fields(streamId), Integer.parseInt(properties
                .getProperty("batchSize")), values);
        spout.setCycle(Boolean.parseBoolean(properties.getProperty("cycle")));

        return topology.newStream(streamId, spout);
    }

}
