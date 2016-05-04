package io.realq.builder.trident.window;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

public class TimeWindowPartitionStateUpdater extends BaseStateUpdater<MapState<List<Object>>> {
    public void updateState(MapState<List<Object>> state, List<TridentTuple> tuples, TridentCollector collector) {
        List<List<Object>> keys = getKeys(tuples);
        List<List<Object>> current = state.multiGet(keys);
        // System.out.println(current);
        Map<List<Object>, List<Object>> map = new HashMap<List<Object>, List<Object>>();
        for (int i = 0; i < current.size(); i++) {
            map.put(keys.get(i), current.get(i));
        }

        for (TridentTuple t : tuples) {
            List<Object> value = map.get(Arrays.asList(t.get(0)));

            if (null == value) {
                value = new ArrayList<Object>();
            }
            if (!value.contains(t.get(1))) {
                value.add(t.get(1));

            }
            map.put(Arrays.asList(t.get(0)), value);

        }
        List<List<Object>> keysOut = new ArrayList<List<Object>>(map.keySet());
        List<List<Object>> valsOut = new ArrayList<List<Object>>(map.values());

        state.multiPut(keysOut, valsOut);
    }

    public static List<List<Object>> getKeys(List<TridentTuple> tuples) {
        List<List<Object>> keys = new ArrayList<List<Object>>();
        for (TridentTuple t : tuples) {
            if (!keys.contains(Arrays.asList(t.get(0))))
                keys.add(Arrays.asList(t.get(0)));
        }

        return keys;
    }
}