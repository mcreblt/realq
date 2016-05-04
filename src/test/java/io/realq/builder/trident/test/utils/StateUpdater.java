package io.realq.builder.trident.test.utils;

import io.realq.parser.expr.Primitive;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class StateUpdater extends BaseStateUpdater<MemoryMapState<Primitive>> implements Serializable {

    private static final long serialVersionUID = 5455344986402189099L;

    String partition;

    public StateUpdater(String partition) {
        this.partition = partition;
    }

    @Override
    public void updateState(MemoryMapState<Primitive> state, List<TridentTuple> tuples, TridentCollector collector) {

        for (TridentTuple tuple : tuples) {
            List<Primitive> vals = new ArrayList<Primitive>();
            Object key = (Primitive) tuple.getValueByField(partition);
            Primitive val = (Primitive) tuple.get(1);
            vals.add(val);
            state.multiPut(Arrays.asList(Arrays.asList(key)), vals);
        }
    }
}
