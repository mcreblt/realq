package io.realq.builder.trident.window;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;

import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class PartitionKeysAggregator extends BaseAggregator<HashSetValuedHashMap<Object, Object>> {

    public HashSetValuedHashMap<Object, Object> init(Object batchId, TridentCollector collector) {
        HashSetValuedHashMap<Object, String> map = new HashSetValuedHashMap<Object, String>();
        return new HashSetValuedHashMap<Object, Object>();
    }

    public void aggregate(HashSetValuedHashMap<Object, Object> state, TridentTuple tuple, TridentCollector collector) {
        state.put(tuple.get(0), tuple.get(1));
    }

    public void complete(HashSetValuedHashMap<Object, Object> state, TridentCollector collector) {
        for (Object k : state.keySet()) {
            for (Object value : state.get(k)) {
                collector.emit(new Values(k, value));
            }
        }
    }
}