	package io.realq.prototypes;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import io.realq.builder.trident.window.PartitionKeysAggregator;
import io.realq.builder.trident.window.PartitionKeysEmit;
import io.realq.builder.trident.window.TimeWindowEmitPartition;
import io.realq.builder.trident.window.TimeWindowPartitionStateCleanup;
import io.realq.builder.trident.window.TimeWindowPartitionStateUpdater;
import io.realq.builder.trident.window.TimeWindowPartitioner;
import io.realq.builder.trident.window.TimeWindowTickSpout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;


public class TimeWindowAggr {

    public static void main(String[] args) throws Exception {

    	TridentTopology topology = new TridentTopology();

        Long baseTime = (Long)(System.currentTimeMillis()/1000);
        Long window = 5L;
        Long hop = 5L;

        Stream streamMain = topology
        		.newStream("stream", new PrototypesBatchSpout(5))
    		    .each(new TimeWindowPartitioner(baseTime, window, hop), new Fields("TimeWindowPartition"))
    		    .partitionBy(new Fields("TimeWindowPartition", "value"))
                .parallelismHint(1)
        		;
       
        TridentState aggregationState = streamMain
                .groupBy(new Fields("TimeWindowPartition", "value"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("TimeWindowPartition"), new Count(), new Fields("count"))
                .parallelismHint(1)
                ;

        // progress lookup state
        TridentState keyState = streamMain
        		.partitionAggregate(new Fields("TimeWindowPartition", "value"), new PartitionKeysAggregator(), new Fields("TimeWindowPartition", "value"))
        		.partitionPersist(new MemoryMapState.Factory(),new Fields("TimeWindowPartition", "value"), new TimeWindowPartitionStateUpdater(), new Fields("groupping", "value"))
                .parallelismHint(3)
                ;
       
        
	    Stream streamTick = topology
	    		.newStream("tick", new TimeWindowTickSpout(1000))
	    		
	    		.each(new Fields("TIMESTAMP_SECONDS"), new TimeWindowEmitPartition(baseTime, window), new Fields("querySeconds"))
	    		
	            .stateQuery(keyState, new Fields("querySeconds"), new MapGet(), new Fields("keys"))
        		.each(new Fields("keys"), new PartitionKeysEmit(), new Fields("key"))

                .stateQuery(aggregationState, new Fields("querySeconds", "key"), new MapGet(), new Fields("count"))
                .stateQuery(keyState, new Fields("querySeconds"), new TimeWindowPartitionStateCleanup(), new Fields())
                .each(new Fields("TIMESTAMP_SECONDS", "querySeconds","key", "count"),new Debug("result"))
                
	    ;    
        
    	
    	Config conf = new Config();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("basic_primitives", conf, topology.build());
        Thread.sleep(100000);
    }
    
}
