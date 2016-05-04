package io.realq.builder;

import io.realq.parser.element.ElementsGraph;

import java.util.List;

public interface TopologyBuilder {
    void build(List<ElementsGraph> graphs);
}
