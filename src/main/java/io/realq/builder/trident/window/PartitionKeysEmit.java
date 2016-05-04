package io.realq.builder.trident.window;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class PartitionKeysEmit extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        if (null != tuple.get(0)) {
            List<Object> keyList = (List<Object>) tuple.get(0);
            for (Object key : keyList) {
                collector.emit(new Values(key));
            }
        }

    }
}