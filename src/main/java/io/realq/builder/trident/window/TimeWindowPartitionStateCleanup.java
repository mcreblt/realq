package io.realq.builder.trident.window;

import java.util.List;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;

import storm.trident.state.map.RemovableMapState;
import storm.trident.tuple.TridentTuple;

public class TimeWindowPartitionStateCleanup extends BaseQueryFunction<RemovableMapState, Object> {

    @Override
    public List<Object> batchRetrieve(RemovableMapState map, List<TridentTuple> keys) {

        map.multiRemove((List) keys);
        return (List) keys;
    }

    @Override
    public void execute(TridentTuple tuple, Object result, TridentCollector collector) {

        collector.emit(new Values(result));
    }

}