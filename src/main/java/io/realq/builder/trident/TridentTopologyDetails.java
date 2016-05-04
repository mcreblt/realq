package io.realq.builder.trident;

import java.util.HashMap;

import storm.trident.Stream;
import storm.trident.TridentTopology;

public class TridentTopologyDetails {
    private HashMap<String, Stream> streams;
    private TridentTopology topology;
    private int parallelism;
    public TridentTopologyDetails(HashMap<String, Stream> streams, TridentTopology topology, int parallelism) {
        super();
        this.streams = streams;
        this.topology = topology;
        this.parallelism = parallelism;
    }

    public Stream getStream(String key) {
        return streams.get(key);
    }

    public Stream putStream(String key, Stream value) {
        return streams.put(key, value);
    }

    public TridentTopology getTopology() {
        return topology;
    }
    public int getParallelism() {
        return parallelism;
    }

    
}
