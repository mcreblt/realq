package io.realq.builder.trident.source;

import java.util.Properties;

import storm.trident.Stream;
import storm.trident.TridentTopology;

public interface Source {
    public Stream getSource(String streamId, TridentTopology topology, Properties properties);
}
