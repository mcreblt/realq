package io.realq.builder.trident;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import io.realq.builder.trident.window.PartitionKeysAggregator;
import io.realq.builder.trident.window.PartitionKeysEmit;
import io.realq.builder.trident.window.TimeWindowEmitPartition;
import io.realq.builder.trident.window.TimeWindowPartitionStateCleanup;
import io.realq.builder.trident.window.TimeWindowPartitionStateUpdater;
import io.realq.builder.trident.window.TimeWindowPartitioner;
import io.realq.builder.trident.window.TimeWindowTickSpout;
import io.realq.parser.element.ElementAggregate;

public class TridentAggregate {
    final static Logger logger = LoggerFactory.getLogger(TridentAggregate.class);
    Stream stream;
    int parallelism;
    TridentTopology topology;
    
    public TridentAggregate(Stream stream, int parallelism, TridentTopology topology) {
        super();
        this.stream = stream;
        this.parallelism = parallelism;
        this.topology = topology;
    }

    public Stream buildStream(ElementAggregate function) {
        
        Long baseTime = (Long) (System.currentTimeMillis() / 1000);
        List<String> streamFields = stream.getOutputFields().toList();
        List<String> newFields = TridentUtils.getFields(function.getOutputs());
        logger.info("Stream fields:" + streamFields.toString() + "; Stream:" + stream);
        newFields.removeAll(streamFields);

        List<String> groupByFields = new ArrayList<String>();
        groupByFields.add(function.getGroupBy().get(0).getStringValue());
        List<String> inputFields = TridentUtils.getFields(function.getInputs());
        CombinerAggregator<Long> tridentFunction = TridentUtils.getFunction(function.getName());
        
        if (null != function.getWindowSize()) {

            stream = timeWindowAggregation(stream, topology, baseTime, function.getWindowSize(),
                    function.getWindowHop(), groupByFields, new Fields(inputFields),
                    tridentFunction, new Fields(newFields));
        } else {
            stream = stream.groupBy(new Fields(groupByFields)).aggregate(new Fields(inputFields),
                    tridentFunction, new Fields(newFields))
            ;

        }

        // next filter all but the last of the window
        ;
        return stream;
    }

    private Stream timeWindowAggregation(Stream stream, TridentTopology topology, Long baseTime, Long window,
            Long hop, List<String> groupByFields, Fields inputs, CombinerAggregator<Long> combinerAggregator,
            Fields output) {
        
        List<String> outputGroupByFields = new ArrayList<>(groupByFields);
        
        groupByFields.add(0, "TIME_WINDOW_PARTITION");
        Stream streamMain = stream
                .each(new TimeWindowPartitioner(baseTime, window, hop), new Fields("TIME_WINDOW_PARTITION"))
                .partitionBy(new Fields(groupByFields)).parallelismHint(1);
        // System.out.println(groupByFields);
        TridentState aggregationState = streamMain
                // .each(new Fields(groupByFields),new Debug("aggr"))
                .groupBy(new Fields(groupByFields))
                .persistentAggregate(new MemoryMapState.Factory(), inputs, combinerAggregator, new Fields("aggregate"))
                .parallelismHint(parallelism);

        // progress lookup state
        TridentState keyState = streamMain
                .partitionAggregate(new Fields(groupByFields), new PartitionKeysAggregator(), new Fields(groupByFields))
                .partitionPersist(new MemoryMapState.Factory(), new Fields(groupByFields), new TimeWindowPartitionStateUpdater(), new Fields(groupByFields))
                .parallelismHint(parallelism);
//        List<String> debug = output.toList();
//
//        debug.add(0, "key");
//        debug.add(0, "TIMESTAMP_SECONDS");
        Stream streamTick = topology
                .newStream("tick", new TimeWindowTickSpout(1000))
                .each(new Fields("TIMESTAMP_SECONDS"), 
                      new TimeWindowEmitPartition(baseTime, window),
                      new Fields("TIME_WINDOW_PARTITION"))

                .stateQuery(keyState, new Fields("TIME_WINDOW_PARTITION"), new MapGet(), new Fields("KEYS"))
                .each(new Fields("KEYS"), new PartitionKeysEmit(), new Fields(outputGroupByFields))

                .stateQuery(aggregationState, new Fields(groupByFields), new MapGet(), output)
                .stateQuery(keyState, new Fields("TIME_WINDOW_PARTITION"), new TimeWindowPartitionStateCleanup(), new Fields())
                
//                .each(new Fields(groupByFields), new Debug("result"))
                ;
        return streamTick;
    }

}
